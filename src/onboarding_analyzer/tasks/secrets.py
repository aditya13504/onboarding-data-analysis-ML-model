from __future__ import annotations
"""Secret rotation & recommendation learning tasks (operational hardening)."""
from datetime import datetime, timedelta
from celery import shared_task
from sqlalchemy.orm import Session
from onboarding_analyzer.infrastructure.db import SessionLocal
from onboarding_analyzer.config import get_settings
from onboarding_analyzer.security.crypto import decrypt_secret, encrypt_secret, EncryptionError
from onboarding_analyzer.models.tables import ConnectorCredential, SecretRotationAudit, SuggestionPattern, RecommendationLearningMetric, RecommendationFeedback, UserRecommendation
from sqlalchemy import func

@shared_task
def rotate_secrets():
    s = get_settings()
    session: Session = SessionLocal()
    try:
        cutoff = datetime.utcnow() - timedelta(days=s.secret_rotation_days)
        creds = session.query(ConnectorCredential).filter(ConnectorCredential.active==1).all()
        rotated = 0
        for c in creds:
            if c.rotated_at and c.rotated_at > cutoff:
                continue
            status = 'success'
            reason = None
            old_version = c.version
            try:
                # decrypt -> re-encrypt (key may have changed if external provider rotated)
                plain = decrypt_secret(c.encrypted_value)
                c.encrypted_value = encrypt_secret(plain)
                c.version += 1
                c.rotated_at = datetime.utcnow()
                rotated += 1
            except EncryptionError as e:
                status = 'failed'
                reason = str(e)
            session.add(SecretRotationAudit(
                connector_name=c.connector_name,
                secret_key=c.secret_key,
                old_version=old_version,
                new_version=c.version if status=='success' else old_version,
                status=status,
                reason=reason,
            ))
        session.commit()
        return {"status": "ok", "rotated": rotated}
    finally:
        session.close()


@shared_task
def learn_recommendation_weights(window_hours: int = 24, min_impressions: int = 5, max_adjust: float = 0.3):
    """Adaptive adjustment of SuggestionPattern weights based on recent feedback & conversions.

    For each recommendation key:
      impressions = count(UserRecommendation where created_at in window)
      accepts = feedback=accept OR converted recommendations
      dismisses = feedback=dismiss
      converts = feedback=convert OR converted recommendations
      ctr = (accepts + converts)/impressions
      conversion_rate = converts/impressions
      weight_adjustment = f(ctr, conversion_rate) relative to global averages.
    Adjustment bounded by +/- max_adjust relative to base_weight.
    """
    session: Session = SessionLocal()
    try:
        end = datetime.utcnow()
        start = end - timedelta(hours=window_hours)
        # Impressions per recommendation
        imp_rows = session.query(UserRecommendation.recommendation, func.count(UserRecommendation.id)).\
            filter(UserRecommendation.created_at >= start, UserRecommendation.created_at < end).group_by(UserRecommendation.recommendation).all()
        if not imp_rows:
            return {"status": "no_data"}
        impressions_map = {r: int(c) for r,c in imp_rows}
        # Feedback
        fb_rows = session.query(RecommendationFeedback.recommendation, RecommendationFeedback.feedback, func.count(RecommendationFeedback.id)).\
            filter(RecommendationFeedback.created_at >= start, RecommendationFeedback.created_at < end).group_by(RecommendationFeedback.recommendation, RecommendationFeedback.feedback).all()
        fb_acc: dict[str, dict[str,int]] = {}
        for rec, fb, cnt in fb_rows:
            fb_acc.setdefault(rec, {})[fb] = int(cnt)
        # Conversions (converted status on recommendations)
        conv_rows = session.query(UserRecommendation.recommendation, func.count(UserRecommendation.id)).\
            filter(UserRecommendation.status=='converted', UserRecommendation.acted_at >= start, UserRecommendation.acted_at < end).group_by(UserRecommendation.recommendation).all()
        conv_map = {r:int(c) for r,c in conv_rows}
        # Global baseline ctr for normalization
        total_impressions = sum(impressions_map.values())
        total_accepts = sum(fb_acc.get(r, {}).get('accept',0) + conv_map.get(r,0) for r in impressions_map)
        global_ctr = (total_accepts / total_impressions) if total_impressions else 0.0
        # Process each suggestion pattern
        patterns = session.query(SuggestionPattern).filter(SuggestionPattern.dynamic_adjust==1).all()
        adjustments = 0
        for pat in patterns:
            imp = impressions_map.get(pat.key, 0)
            if imp < min_impressions:
                continue
            accepts = fb_acc.get(pat.key, {}).get('accept',0) + conv_map.get(pat.key,0)
            dismisses = fb_acc.get(pat.key, {}).get('dismiss',0)
            converts = conv_map.get(pat.key,0)
            ctr = accepts/imp if imp else 0.0
            conversion_rate = converts/imp if imp else 0.0
            uplift = ctr - global_ctr
            # baseline weight reference
            if pat.base_weight is None:
                pat.base_weight = pat.weight
            target = pat.base_weight * (1 + max(-max_adjust, min(max_adjust, uplift)))
            if abs(target - pat.weight) / max(1e-6, pat.weight) > 0.02:
                pat.weight = target
                adjustments += 1
            # persist learning metric row
            session.add(RecommendationLearningMetric(
                recommendation=pat.key,
                window_start=start,
                window_end=end,
                impressions=imp,
                accepts=accepts,
                dismisses=dismisses,
                converts=converts,
                ctr=ctr,
                conversion_rate=conversion_rate,
                weight_adjustment=pat.weight - (pat.base_weight or pat.weight),
            ))
        session.commit()
        return {"status": "ok", "patterns": len(patterns), "adjusted": adjustments}
    finally:
        session.close()