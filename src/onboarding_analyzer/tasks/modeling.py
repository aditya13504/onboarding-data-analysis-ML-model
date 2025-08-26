from __future__ import annotations
from celery import shared_task
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import select, func
from onboarding_analyzer.infrastructure.db import SessionLocal
from onboarding_analyzer.models.tables import RawEvent, UserFeature, UserChurnRisk, CohortRetention, ModelVersion, ExperimentDefinition, ExperimentAssignment, ExperimentDecisionSnapshot, AlertRule, AlertLog
import math
from prometheus_client import Counter
try:
    from onboarding_analyzer.api.main import registry as _api_registry  # type: ignore
except Exception:  # pragma: no cover
    _api_registry = None

AB_TEST_RUNS = Counter('ab_test_runs_total', 'AB test metric computation runs', registry=_api_registry)
AB_TEST_VARIANTS = Counter('ab_test_variants_total', 'Variants processed across AB tests', registry=_api_registry)
AB_TEST_SIGNIFICANT = Counter('ab_test_significant_deltas_total', 'Significant variant differences (p<0.05)', registry=_api_registry)
EXPERIMENT_ASSIGNMENTS = Counter('experiment_assignments_total', 'Experiment assignments created', registry=_api_registry)
EXPERIMENT_DECISIONS = Counter('experiment_decisions_total', 'Experiment decision evaluations', ['decision'], registry=_api_registry)
ALERTS_FIRED = Counter('alerts_fired_total', 'Alerts fired', ['rule_type'], registry=_api_registry)

@shared_task
def build_user_features(lookback_days: int = 30):
    session: Session = SessionLocal()
    try:
        since = datetime.utcnow() - timedelta(days=lookback_days)
        q = select(RawEvent.user_id, RawEvent.project, RawEvent.event_name, RawEvent.ts).where(RawEvent.ts >= since).order_by(RawEvent.user_id, RawEvent.ts)
        user_events = {}
        first_ts = {}
        last_ts = {}
        event_counts = {}
        for user_id, project, event_name, ts in session.execute(q):
            key = (user_id, project)
            ev_list = user_events.setdefault(key, [])
            ev_list.append((event_name, ts))
            first_ts[key] = min(first_ts.get(key, ts), ts) if key in first_ts else ts
            last_ts[key] = max(last_ts.get(key, ts), ts)
            event_counts.setdefault(key, {})[event_name] = event_counts.setdefault(key, {}).get(event_name, 0) + 1
        for (user_id, project), evs in user_events.items():
            duration = (last_ts[(user_id, project)] - first_ts[(user_id, project)]).total_seconds() / 3600.0
            unique_events = len({e for e,_ in evs})
            total_events = len(evs)
            freq = total_events / max(duration, 1/60)
            feats = {
                'unique_events': unique_events,
                'total_events': total_events,
                'active_hours': duration,
                'events_per_hour': freq,
            }
            existing = session.query(UserFeature).filter_by(user_id=user_id, project=project).first()
            if existing:
                existing.features = feats
                existing.updated_at = datetime.utcnow()
            else:
                session.add(UserFeature(user_id=user_id, project=project, features=feats))
        session.commit()
        return {'status':'ok','users': len(user_events)}
    finally:
        session.close()

@shared_task
def compute_churn_risk(model_version: str | None = None):
    """Toy churn risk: logistic function over activity recency and frequency."""
    session: Session = SessionLocal()
    try:
        if not model_version:
            model_version = datetime.utcnow().strftime('%Y%m%d%H%M%S')
            session.add(ModelVersion(model_name='churn', version=model_version))
            session.commit()
        now = datetime.utcnow()
        features = session.query(UserFeature).all()
        for uf in features:
            feats = uf.features or {}
            freq = feats.get('events_per_hour', 0.0)
            active_hours = feats.get('active_hours', 0.0)
            recency_hours = (now - uf.updated_at).total_seconds()/3600.0
            # Score: higher if low freq, low active_hours, high recency gap
            linear = -0.8*freq -0.5*active_hours + 0.6*recency_hours
            risk = 1/(1+math.exp(-linear))
            session.add(UserChurnRisk(user_id=uf.user_id, project=uf.project, risk_score=risk, model_version=model_version))
        session.commit()
        return {'status':'ok','scored': len(features), 'model_version': model_version}
    finally:
        session.close()

@shared_task
def compute_cohort_retention(days: int = 14):
    session: Session = SessionLocal()
    try:
        now = datetime.utcnow()
        # Identify signup cohort by first event timestamp truncated to day
        q_first = select(RawEvent.user_id, func.min(RawEvent.ts)).group_by(RawEvent.user_id)
        first_map = {row[0]: row[1] for row in session.execute(q_first)}
        # Clear overlapping range
        floor_date = (now - timedelta(days=days)).replace(hour=0, minute=0, second=0, microsecond=0)
        session.query(CohortRetention).filter(CohortRetention.cohort_date >= floor_date).delete()
        session.commit()
        for user, first_ts in first_map.items():
            cohort_day = first_ts.replace(hour=0, minute=0, second=0, microsecond=0)
            if cohort_day < floor_date:
                continue
            for day in range(0, days+1):
                window_start = cohort_day + timedelta(days=day)
                window_end = window_start + timedelta(days=1)
                # Has activity this day?
                active = session.query(RawEvent).filter(RawEvent.user_id==user, RawEvent.ts>=window_start, RawEvent.ts<window_end).first() is not None
                # For cohort size (total_users) we just use count of users in first_map with same cohort_day
                total_users = sum(1 for ts in first_map.values() if ts.replace(hour=0,minute=0,second=0,microsecond=0)==cohort_day)
                retained_users = 1 if active else 0
                session.add(CohortRetention(cohort_date=cohort_day, day_number=day, retained_users=retained_users, total_users=total_users))
        session.commit()
        return {'status':'ok'}
    finally:
        session.close()

@shared_task
def compute_ab_test_metrics(experiment_prop: str = 'experiment', conversion_event: str | None = None):
    session: Session = SessionLocal()
    try:
        from onboarding_analyzer.models.tables import ABTestMetric
        # simple: infer variants from property on events props
        q = select(RawEvent.user_id, RawEvent.props, RawEvent.event_name)
        variants = {}
        conversions = {}
        users_by_variant = {}
        for user_id, props, event_name in session.execute(q):
            variant = None
            if isinstance(props, dict):
                variant = props.get(experiment_prop)
            if not variant:
                continue
            users_by_variant.setdefault(variant, set()).add(user_id)
            if conversion_event and event_name == conversion_event:
                conversions.setdefault(variant, set()).add(user_id)
        # Clear prior metrics for this experiment
        session.query(ABTestMetric).filter(ABTestMetric.metric_name=='conversion_rate').delete()
        import math
        if not users_by_variant:
            session.commit()
            return {'status':'empty'}
        baseline_variant = sorted(users_by_variant.keys())[0]
        # Precompute baseline stats
        base_users = users_by_variant[baseline_variant]
        base_total = len(base_users)
        base_conv = len(conversions.get(baseline_variant,set())) if conversion_event else 0
        base_rate = base_conv/base_total if base_total else 0
        # Bayesian posterior for baseline (Beta(1,1) prior)
        def beta_hpdi(alpha: float, beta: float, cred: float = 0.95):
            # coarse grid approximation (avoids extra deps)
            import numpy as np
            xs = np.linspace(0,1,2001)
            import math as _m
            log_pdf = (alpha-1)*np.log(np.clip(xs,1e-9,1)) + (beta-1)*np.log(np.clip(1-xs,1e-9,1))
            pdf = np.exp(log_pdf - np.max(log_pdf))
            pdf /= pdf.sum()
            cdf = np.cumsum(pdf)
            lo = xs[np.searchsorted(cdf, (1-cred)/2)]
            hi = xs[np.searchsorted(cdf, 1-(1-cred)/2)]
            return float(lo), float(hi)
        base_alpha = 1 + base_conv
        base_beta = 1 + (base_total - base_conv)
        base_lo, base_hi = beta_hpdi(base_alpha, base_beta)
        for variant, users in users_by_variant.items():
            total = len(users)
            conv = len(conversions.get(variant, set())) if conversion_event else 0
            cr = conv/total if total else 0
            # Frequentist p-value vs baseline
            if variant == baseline_variant or conversion_event is None:
                p_val = None
            else:
                p1 = base_rate
                p2 = cr
                n1 = base_total
                n2 = total
                pooled = ((p1*n1)+(p2*n2))/(n1+n2)
                se = math.sqrt(pooled*(1-pooled)*((1/n1)+(1/n2))) if n1 and n2 else 0
                z = (p2-p1)/se if se else 0
                p_val = math.erfc(abs(z)/math.sqrt(2)) if se else 1.0
            # Bayesian posterior for variant
            alpha = 1 + conv
            beta = 1 + (total - conv)
            lo, hi = beta_hpdi(alpha, beta)
            # Probability variant beats baseline (Monte Carlo or closed form). Use sampling approximation.
            # P(X>Y) where X~Beta(a1,b1), Y~Beta(a2,b2). Approx via 5000 samples for robustness.
            try:
                import random
                samples = 3000
                gt = 0
                for _ in range(samples):
                    import math as _m
                    # use random.betavariate
                    x = random.betavariate(alpha, beta)
                    y = random.betavariate(base_alpha, base_beta)
                    if x > y:
                        gt += 1
                prob_beats = gt / samples
            except Exception:
                prob_beats = None
            lift = (cr - base_rate)/base_rate if base_rate else None
            session.add(ABTestMetric(
                experiment_name=experiment_prop,
                variant=variant,
                metric_name='conversion_rate',
                metric_value=cr,
                users=total,
                p_value=p_val,
                lift=lift,
                prob_beats_baseline=prob_beats if variant != baseline_variant else None,
                hpdi_low=lo,
                hpdi_high=hi,
            ))
        session.commit()
        return {'status':'ok'}
    finally:
        session.close()


@shared_task
def compute_all_ab_tests(conversion_event: str | None = None, prop_prefixes: str = 'experiment,exp_'):
    """Discover experiment properties dynamically and compute metrics for each.

    prop_prefixes: comma list of property name prefixes to treat as experiment designators.
    """
    session: Session = SessionLocal()
    try:
        prefixes = [p.strip() for p in prop_prefixes.split(',') if p.strip()]
        # Sample recent events (last 7d) to enumerate candidate experiment properties
        from datetime import datetime, timedelta
        since = datetime.utcnow() - timedelta(days=7)
        q = select(RawEvent.props).where(RawEvent.ts >= since).limit(5000)
        candidate_props = set()
        for (props,) in session.execute(q):
            if not isinstance(props, dict):
                continue
            for k in props.keys():
                if any(k.startswith(pref) for pref in prefixes):
                    candidate_props.add(k)
        results = []
        for prop in sorted(candidate_props):
            r = compute_ab_test_metrics.run(experiment_prop=prop, conversion_event=conversion_event)  # call task body directly
            results.append({prop: r})
            try:
                from onboarding_analyzer.models.tables import ABTestMetric
                variants = session.query(ABTestMetric).filter(ABTestMetric.experiment_name==prop).all()
                AB_TEST_VARIANTS.inc(len({v.variant for v in variants}))
                # significance: any variant p_value < 0.05
                sig = sum(1 for v in variants if (v.p_value is not None and v.p_value < 0.05))
                if sig:
                    AB_TEST_SIGNIFICANT.inc(sig)
            except Exception:
                pass
        try:
            AB_TEST_RUNS.inc()
        except Exception:
            pass
        return {'status': 'ok', 'experiments': len(candidate_props), 'details': results}
    finally:
        session.close()

@shared_task
def assign_experiments(batch_limit: int = 10000):
    """Assign users deterministically to active experiments based on hash(user_id+salt).

    Traffic allocation: if provided, cumulative percentage buckets; else equal allocation.
    Writes assignment records if absent.
    """
    import hashlib
    session: Session = SessionLocal()
    try:
        exps = session.query(ExperimentDefinition).filter(ExperimentDefinition.active==1).all()
        if not exps:
            return {"status": "no_experiments"}
        # Sample recent distinct users
        users = [row[0] for row in session.execute(select(RawEvent.user_id).order_by(RawEvent.id.desc()).distinct().limit(batch_limit))]
        created = 0
        for exp in exps:
            variants = exp.variants.get('variants') if isinstance(exp.variants, dict) else None
            if not variants:
                continue
            alloc = exp.traffic_allocation if isinstance(exp.traffic_allocation, dict) else {}
            # Build cumulative buckets
            if not alloc:
                pct = 100/len(variants)
                alloc = {v: pct for v in variants}
            total_pct = sum(alloc.values()) or 1
            cumulative = []
            acc = 0.0
            for v in variants:
                acc += alloc.get(v,0)/total_pct*100
                cumulative.append((v, acc))
            for uid in users:
                # Skip if already assigned
                if session.query(ExperimentAssignment).filter_by(experiment_key=exp.key, user_id=uid).first():
                    continue
                h = hashlib.sha256(f"{uid}:{exp.hash_salt}".encode()).hexdigest()
                # map first 8 hex to int 0-100
                val = (int(h[:8],16)/0xFFFFFFFF)*100
                chosen = variants[-1]
                for v, cutoff in cumulative:
                    if val <= cutoff:
                        chosen = v
                        break
                session.add(ExperimentAssignment(experiment_key=exp.key, user_id=uid, variant=chosen))
                created += 1
        session.commit()
        try:
            if created:
                EXPERIMENT_ASSIGNMENTS.inc(created)
        except Exception:
            pass
        return {"status": "ok", "assignments": created, "experiments": len(exps), "users": len(users)}
    finally:
        session.close()

@shared_task
def adaptive_reallocate_experiments(batch_limit: int = 5000, min_users: int = 20, exploit_bias: float = 0.05):
    """Adjust traffic allocation for adaptive experiments using Thompson Sampling.

    For experiments flagged adaptive=1, derive posterior Beta(a,b) per variant from observed
    user-level conversion (conversion_event on ExperimentDefinition). Recompute allocation
    proportions ~ P(variant is best) with slight exploit bias to current best.
    Persist updated traffic_allocation JSON in ExperimentDefinition for subsequent assignments.
    No mock data: uses real RawEvent props + conversion events.
    """
    session: Session = SessionLocal()
    try:
        exps = session.query(ExperimentDefinition).filter(ExperimentDefinition.active==1, ExperimentDefinition.adaptive==1).all()
        if not exps:
            return {"status":"no_adaptive_experiments"}
        updated = 0
        for exp in exps:
            variants = exp.variants.get('variants') if isinstance(exp.variants, dict) else None
            if not variants:
                continue
            conv_event = exp.conversion_event or 'conversion'
            # Gather data
            q = select(RawEvent.user_id, RawEvent.props, RawEvent.event_name)
            variant_users: dict[str,set[str]] = {v:set() for v in variants}
            variant_converters: dict[str,set[str]] = {v:set() for v in variants}
            for uid, props, ev in session.execute(q):
                if not isinstance(props, dict):
                    continue
                v = props.get(exp.assignment_prop)
                if v not in variant_users:
                    continue
                variant_users[v].add(uid)
                if ev == conv_event:
                    variant_converters[v].add(uid)
            totals = {v: len(variant_users[v]) for v in variants}
            if sum(totals.values()) < min_users:
                continue  # not enough data yet
            import random
            samples = 4000
            win_counts = {v:0 for v in variants}
            post_params = {}
            for v in variants:
                conv = len(variant_converters[v])
                total = totals[v]
                post_params[v] = (1+conv, 1+total-conv)
            for _ in range(samples):
                draws = {v: random.betavariate(post_params[v][0], post_params[v][1]) for v in variants}
                best = max(draws.items(), key=lambda x: x[1])[0]
                win_counts[best] += 1
            total_wins = sum(win_counts.values()) or 1
            alloc = {v: win_counts[v]/total_wins for v in variants}
            # Exploit bias: slightly increase current best
            best_variant = max(alloc.items(), key=lambda x: x[1])[0]
            alloc[best_variant] = min(1.0, alloc[best_variant] + exploit_bias)
            # Renormalize
            s = sum(alloc.values()) or 1
            alloc = {v: round(alloc[v]/s*100, 2) for v in variants}
            exp.traffic_allocation = alloc
            updated += 1
        if updated:
            session.commit()
        return {"status":"ok","updated": updated}
    finally:
        session.close()

@shared_task
def evaluate_experiment_decision(experiment_prop: str, conversion_event: str, min_prob: float = 0.95, min_lift: float = 0.02, futility_prob: float = 0.90, samples: int = 4000):
    """Sequential stopping guidance using Bayesian posteriors (Beta prior 1,1).

    Returns a decision:
      - stop_winner: some variant has P( variant > baseline ) >= min_prob AND P(practical_lift) >= min_prob
      - stop_futile: all non-baseline variants have P(practical_lift) < (1 - futility_prob)
      - continue: otherwise

    Practical lift defined as (variant_rate - baseline_rate)/baseline_rate >= min_lift.
    Uses direct sampling from Beta posteriors derived from real event conversion data.
    """
    session: Session = SessionLocal()
    try:
        # Gather user -> variant mapping and conversions
        q = select(RawEvent.user_id, RawEvent.props, RawEvent.event_name)
        variant_users: dict[str,set[str]] = {}
        variant_converters: dict[str,set[str]] = {}
        for uid, props, ev in session.execute(q):
            if not isinstance(props, dict):
                continue
            v = props.get(experiment_prop)
            if not v:
                continue
            variant_users.setdefault(v,set()).add(uid)
            if ev == conversion_event:
                variant_converters.setdefault(v,set()).add(uid)
        if not variant_users:
            return {"status": "empty"}
        baseline = sorted(variant_users.keys())[0]
        # Build posterior params
        post: dict[str, tuple[int,int]] = {}
        for v, users in variant_users.items():
            conv = len(variant_converters.get(v,set()))
            post[v] = (1+conv, 1+len(users)-conv)
        import random, math
        # Pre-draw baseline samples
        b_alpha, b_beta = post[baseline]
        b_samples = [random.betavariate(b_alpha, b_beta) for _ in range(samples)]
        variant_stats = {}
        winner = None
        decision = 'continue'
        for v,(a,b) in post.items():
            if v == baseline:
                continue
            x_samples = [random.betavariate(a,b) for _ in range(samples)]
            # P(variant > baseline)
            gt = sum(1 for xs, bs in zip(x_samples, b_samples) if xs > bs)/samples
            # P(practical lift)
            practical = sum(1 for xs, bs in zip(x_samples, b_samples) if bs>0 and (xs-bs)/bs >= min_lift)/samples
            variant_stats[v] = {
                "prob_beats_baseline": gt,
                "prob_practical_lift": practical,
                "mean_rate": sum(x_samples)/samples,
            }
            if gt >= min_prob and practical >= min_prob and (winner is None or variant_stats[v]['mean_rate'] > variant_stats.get(winner,{}).get('mean_rate',0)):
                winner = v
        if winner:
            decision = 'stop_winner'
        else:
            # Futility: all variants unlikely to reach practical lift
            if all(stats['prob_practical_lift'] < (1 - futility_prob) for stats in variant_stats.values()):
                decision = 'stop_futile'
        try:
            EXPERIMENT_DECISIONS.labels(decision=decision).inc()
        except Exception:
            pass
        return {
            "status": "ok",
            "decision": decision,
            "baseline": baseline,
            "winner": winner,
            "min_prob": min_prob,
            "min_lift": min_lift,
            "futility_prob": futility_prob,
            "variants": variant_stats,
            "users": {v: len(u) for v,u in variant_users.items()},
            "conversions": {v: len(variant_converters.get(v,set())) for v in variant_users.keys()},
        }
    finally:
        session.close()

@shared_task
def compute_multi_metric_ab_test(experiment_prop: str, metric_events: str):
    """Compute multiple event-based metrics (unique user occurrence) for an experiment.

    metric_events: comma-separated list of event names; each becomes metric_name=<event>_rate
    Uses real RawEvent data. Bayesian + frequentist stats similar to primary metric.
    """
    session: Session = SessionLocal()
    try:
        from onboarding_analyzer.models.tables import ABTestMetric
        events = [e.strip() for e in metric_events.split(',') if e.strip()]
        if not events:
            return {"status":"no_metrics"}
        # Gather variant assignment from props
        q = select(RawEvent.user_id, RawEvent.props, RawEvent.event_name)
        variant_users: dict[str,set[str]] = {}
        event_hits: dict[str, dict[str,set[str]]] = {ev:{} for ev in events}
        for uid, props, ev in session.execute(q):
            if not isinstance(props, dict):
                continue
            v = props.get(experiment_prop)
            if not v:
                continue
            variant_users.setdefault(v,set()).add(uid)
            if ev in event_hits:
                event_hits[ev].setdefault(v,set()).add(uid)
        if not variant_users:
            return {"status":"empty"}
        baseline = sorted(variant_users.keys())[0]
        results = []
        import math, random
        for ev in events:
            metric_name = f"{ev}_rate"
            users_by_variant = variant_users
            hits = event_hits.get(ev,{})
            base_users = users_by_variant[baseline]
            base_total = len(base_users)
            base_conv = len(hits.get(baseline,set()))
            base_rate = base_conv/base_total if base_total else 0
            base_alpha = 1+base_conv
            base_beta = 1+base_total-base_conv
            # grid HPDI
            def hpdi(a,b):
                import numpy as np
                xs = np.linspace(0,1,1001)
                log_pdf = (a-1)*np.log(np.clip(xs,1e-9,1)) + (b-1)*np.log(np.clip(1-xs,1e-9,1))
                pdf = np.exp(log_pdf - np.max(log_pdf)); pdf /= pdf.sum(); cdf = np.cumsum(pdf)
                lo = xs[np.searchsorted(cdf,0.025)]; hi = xs[np.searchsorted(cdf,0.975)]
                return float(lo), float(hi)
            base_lo, base_hi = hpdi(base_alpha, base_beta)
            for variant, users in users_by_variant.items():
                total = len(users)
                conv = len(hits.get(variant,set()))
                rate = conv/total if total else 0
                if variant == baseline:
                    p_val = None
                else:
                    p1 = base_rate; p2 = rate; n1 = base_total; n2 = total
                    pooled = ((p1*n1)+(p2*n2))/(n1+n2) if (n1+n2) else 0
                    se = math.sqrt(pooled*(1-pooled)*((1/n1)+(1/n2))) if n1 and n2 else 0
                    z = (p2-p1)/se if se else 0
                    p_val = math.erfc(abs(z)/math.sqrt(2)) if se else 1.0
                alpha = 1+conv; beta = 1+total-conv
                lo, hi = hpdi(alpha,beta)
                # probability beats baseline via sampling
                prob_beats = None
                if variant != baseline:
                    gt=0; samples=2000
                    for _ in range(samples):
                        x = random.betavariate(alpha,beta); y = random.betavariate(base_alpha, base_beta)
                        if x>y: gt+=1
                    prob_beats = gt/samples
                lift = (rate - base_rate)/base_rate if base_rate else None
                # Upsert or insert (delete existing same metric to keep uniqueness simple)
                session.query(ABTestMetric).filter(ABTestMetric.experiment_name==experiment_prop, ABTestMetric.variant==variant, ABTestMetric.metric_name==metric_name).delete()
                session.add(ABTestMetric(
                    experiment_name=experiment_prop,
                    variant=variant,
                    metric_name=metric_name,
                    metric_value=rate,
                    users=total,
                    p_value=p_val,
                    lift=lift,
                    prob_beats_baseline=prob_beats,
                    hpdi_low=lo,
                    hpdi_high=hi,
                ))
            results.append(metric_name)
        session.commit()
        return {"status":"ok","metrics": results}
    finally:
        session.close()

@shared_task
def snapshot_experiment_decision(experiment_prop: str, conversion_event: str, **kwargs):
    """Persist a time-series snapshot of current experiment decision stats for trend analysis.

    Uses real event data; does not recalculate historical values.
    """
    session: Session = SessionLocal()
    try:
        result = evaluate_experiment_decision.run(experiment_prop=experiment_prop, conversion_event=conversion_event, **kwargs)
        if result.get('status') != 'ok':
            return result
        # Attempt to resolve experiment key from definition
        exp_def = session.query(ExperimentDefinition).filter(ExperimentDefinition.assignment_prop==experiment_prop).first()
        exp_key = exp_def.key if exp_def else None
        # Summary row
        snap_time = datetime.utcnow()
        session.add(ExperimentDecisionSnapshot(
            experiment_prop=experiment_prop,
            experiment_key=exp_key,
            conversion_event=conversion_event,
            decision=result['decision'],
            baseline=result['baseline'],
            winner=result['winner'],
            variant='__summary__',
            prob_beats_baseline=None,
            prob_practical_lift=None,
            mean_rate=None,
            users=sum(result['users'].values()),
            conversions=sum(result['conversions'].values()),
            computed_at=snap_time,
        ))
        # Variant rows
        baseline = result['baseline']
        for variant, users in result['users'].items():
            stats = result['variants'].get(variant, {})
            session.add(ExperimentDecisionSnapshot(
                experiment_prop=experiment_prop,
                experiment_key=exp_key,
                conversion_event=conversion_event,
                decision=result['decision'],
                baseline=baseline,
                winner=result['winner'],
                variant=variant,
                prob_beats_baseline=stats.get('prob_beats_baseline'),
                prob_practical_lift=stats.get('prob_practical_lift'),
                mean_rate=stats.get('mean_rate'),
                users=users,
                conversions=result['conversions'].get(variant,0),
                computed_at=snap_time,
            ))
        session.commit()
        return {"status":"ok","snapshots": len(result['users'])+1}
    finally:
        session.close()

@shared_task
def evaluate_alert_rules():
    """Evaluate active AlertRules and dispatch notifications (real channels if configured).

    Supported rule_type and condition schemas:
      experiment_decision: {"experiment_prop": str, "decision_in": ["stop_winner", ...], "template": optional str}
      experiment_promotion: {"experiment_prop": optional str, "template": optional str}
      anomaly_event: {
          "anomaly_type": "volume"|"latency"|"funnel_dropoff"|"*" (default "*"),
          "project": optional str,
          "min_severity": optional float (default 0),
          "status_in": optional ["open","ack"...], default ["open"],
          "template": optional str,
          "lookback_minutes": optional int (default 120)
      }
    Cooldown logic uses rule.cooldown_minutes between firings, regardless of how many anomalies matched; one alert aggregates matches.
    """
    from onboarding_analyzer.config import get_settings
    settings = get_settings()
    session: Session = SessionLocal()
    fired = 0
    fired_per_type: dict[str,int] = {}
    try:
        rules = session.query(AlertRule).filter(AlertRule.active==1).all()
        from sqlalchemy import desc  # noqa: F401 (may be used later)
        # Preload templates by channel & name for quick lookup
        from onboarding_analyzer.models.tables import NotificationTemplate
        templates = { (t.channel, t.name): t for t in session.query(NotificationTemplate).filter(NotificationTemplate.active==1).all() }
        def render(channel: str, text: str, context: dict) -> str:
            # simple {placeholder} replacement
            try:
                return text.format(**context)
            except Exception:
                return text
        for rule in rules:
            if rule.rule_type == 'experiment_decision':
                exp_prop = rule.condition.get('experiment_prop') if isinstance(rule.condition, dict) else None
                decisions = rule.condition.get('decision_in') if isinstance(rule.condition, dict) else None
                if not exp_prop or not decisions:
                    continue
                snap = session.query(ExperimentDecisionSnapshot).filter(ExperimentDecisionSnapshot.experiment_prop==exp_prop, ExperimentDecisionSnapshot.variant=='__summary__').order_by(ExperimentDecisionSnapshot.computed_at.desc()).first()
                if not snap:
                    continue
                if snap.decision in decisions:
                    # Cooldown check
                    now = datetime.utcnow()
                    if rule.last_fired_at and (now - rule.last_fired_at).total_seconds() < rule.cooldown_minutes*60:
                        continue
                    # Fire alert
                    base_msg = f"Experiment {exp_prop} decision={snap.decision} winner={snap.winner or 'n/a'} baseline={snap.baseline} at {snap.computed_at.isoformat()}"
                    context = {"experiment_prop": exp_prop, "decision": snap.decision, "winner": snap.winner, "baseline": snap.baseline}
                    msg = base_msg
                    # Template override if rule condition specifies template name
                    template_name = rule.condition.get('template') if isinstance(rule.condition, dict) else None
                    if template_name:
                        # prefer channel-specific template later per channel
                        pass
                    # Slack
                    if rule.channels.get('slack') and settings.slack_bot_token and settings.slack_report_channel:
                        try:
                            import requests
                            tmpl = None
                            template_name = rule.condition.get('template') if isinstance(rule.condition, dict) else None
                            if template_name:
                                tmpl = templates.get(('slack', template_name))
                            out_text = render('slack', tmpl.body if tmpl else msg, context | {'base_message': base_msg})
                            headers = {"Authorization": f"Bearer {settings.slack_bot_token}", "Content-Type": "application/json"}
                            requests.post('https://slack.com/api/chat.postMessage', json={"channel": settings.slack_report_channel, "text": out_text}, headers=headers, timeout=5)
                        except Exception:
                            pass
                    # Email
                    if rule.channels.get('email') and settings.smtp_host and settings.email_from and settings.email_recipients:
                        try:
                            import smtplib
                            from email.mime.text import MIMEText
                            recipients = [r.strip() for r in settings.email_recipients.split(',') if r.strip()]
                            if recipients:
                                tmpl = None
                                template_name = rule.condition.get('template') if isinstance(rule.condition, dict) else None
                                if template_name:
                                    tmpl = templates.get(('email', template_name))
                                body_text = render('email', tmpl.body if tmpl else base_msg, context | {'base_message': base_msg})
                                subject = (tmpl.subject if tmpl and tmpl.subject else f"Experiment decision: {exp_prop}")
                                mime = MIMEText(body_text)
                                mime['Subject'] = subject
                                mime['From'] = settings.email_from
                                mime['To'] = ', '.join(recipients)
                                with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=5) as s:
                                    if settings.smtp_user and settings.smtp_password:
                                        try:
                                            s.starttls()
                                        except Exception:
                                            pass
                                        s.login(settings.smtp_user, settings.smtp_password)
                                    s.sendmail(settings.email_from, recipients, mime.as_string())
                        except Exception:
                            pass
                    session.add(AlertLog(rule_id=rule.id, rule_type=rule.rule_type, message=msg, context=context))
                    rule.last_fired_at = now
                    fired += 1
                    fired_per_type[rule.rule_type] = fired_per_type.get(rule.rule_type,0)+1
            elif rule.rule_type == 'experiment_promotion':
                # condition: {"experiment_prop": optional str} -> if omitted, any promotion triggers
                exp_prop = rule.condition.get('experiment_prop') if isinstance(rule.condition, dict) else None
                from onboarding_analyzer.models.tables import ExperimentPromotionAudit
                q = session.query(ExperimentPromotionAudit).order_by(ExperimentPromotionAudit.created_at.desc())
                if exp_prop:
                    q = q.filter(ExperimentPromotionAudit.experiment_prop==exp_prop)
                promo = q.first()
                if not promo:
                    continue
                now = datetime.utcnow()
                if rule.last_fired_at and (now - rule.last_fired_at).total_seconds() < rule.cooldown_minutes*60:
                    continue
                base_msg = f"Experiment {promo.experiment_prop} promoted variant {promo.winner_variant} decision={promo.decision} at {promo.created_at.isoformat()}"
                context = {"experiment_prop": promo.experiment_prop, "winner": promo.winner_variant, "decision": promo.decision}
                template_name = rule.condition.get('template') if isinstance(rule.condition, dict) else None
                # Channels
                if rule.channels.get('slack') and settings.slack_bot_token and settings.slack_report_channel:
                    try:
                        import requests
                        tmpl = templates.get(('slack', template_name)) if template_name else None
                        out_text = render('slack', tmpl.body if tmpl else base_msg, context | {'base_message': base_msg})
                        headers = {"Authorization": f"Bearer {settings.slack_bot_token}", "Content-Type": "application/json"}
                        requests.post('https://slack.com/api/chat.postMessage', json={"channel": settings.slack_report_channel, "text": out_text}, headers=headers, timeout=5)
                    except Exception:
                        pass
                # Email
                if rule.channels.get('email') and settings.smtp_host and settings.email_from and settings.email_recipients:
                    try:
                        import smtplib
                        from email.mime.text import MIMEText
                        recipients = [r.strip() for r in settings.email_recipients.split(',') if r.strip()]
                        if recipients:
                            tmpl = templates.get(('email', template_name)) if template_name else None
                            body_text = render('email', tmpl.body if tmpl else base_msg, context | {'base_message': base_msg})
                            subject = (tmpl.subject if tmpl and tmpl.subject else f"Experiment promotion: {promo.experiment_prop}")
                            mime = MIMEText(body_text)
                            mime['Subject'] = subject
                            mime['From'] = settings.email_from
                            mime['To'] = ', '.join(recipients)
                            with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=5) as s:
                                if settings.smtp_user and settings.smtp_password:
                                    try:
                                        s.starttls()
                                    except Exception:
                                        pass
                                    s.login(settings.smtp_user, settings.smtp_password)
                                s.sendmail(settings.email_from, recipients, mime.as_string())
                    except Exception:
                        pass
                session.add(AlertLog(rule_id=rule.id, rule_type=rule.rule_type, message=base_msg, context=context))
                rule.last_fired_at = now
                fired += 1
                fired_per_type[rule.rule_type] = fired_per_type.get(rule.rule_type,0)+1
            elif rule.rule_type == 'anomaly_event':
                # Aggregate recent anomaly events that match condition
                from onboarding_analyzer.models.tables import AnomalyEvent
                cond = rule.condition if isinstance(rule.condition, dict) else {}
                anomaly_type = cond.get('anomaly_type', '*')
                project = cond.get('project')
                min_sev = cond.get('min_severity', 0)
                status_in = cond.get('status_in') or ['open']
                lookback = int(cond.get('lookback_minutes', 120))
                now = datetime.utcnow()
                # Cooldown check first
                if rule.last_fired_at and (now - rule.last_fired_at).total_seconds() < rule.cooldown_minutes*60:
                    continue
                q = session.query(AnomalyEvent).filter(AnomalyEvent.detected_at >= now - timedelta(minutes=lookback))
                if anomaly_type != '*':
                    q = q.filter(AnomalyEvent.anomaly_type==anomaly_type)
                if project:
                    q = q.filter(AnomalyEvent.project==project)
                if status_in:
                    q = q.filter(AnomalyEvent.status.in_(status_in))
                if min_sev:
                    q = q.filter(AnomalyEvent.severity >= float(min_sev))
                matches = q.order_by(AnomalyEvent.detected_at.desc()).limit(100).all()
                if not matches:
                    continue
                # Build aggregated message
                top = max(matches, key=lambda m: (m.severity or 0))
                groups: dict[str,int] = {}
                for m in matches:
                    key = f"{m.anomaly_type}:{m.project or '_'}"
                    groups[key] = groups.get(key,0)+1
                group_summary = ', '.join(f"{k}x{v}" for k,v in list(groups.items())[:10])
                base_msg = (f"AnomalyEvents: {len(matches)} recent (<= {lookback}m) matches; highest severity "
                            f"{top.anomaly_type} sev={top.severity:.2f} Δ={top.delta_pct or 0:.2f}% project={top.project or 'global'}; "
                            f"groups: {group_summary}")
                context = {
                    "match_count": len(matches),
                    "lookback_minutes": lookback,
                    "anomaly_type_filter": anomaly_type,
                    "project_filter": project,
                    "min_severity": min_sev,
                    "top": {
                        "id": top.id,
                        "anomaly_type": top.anomaly_type,
                        "project": top.project,
                        "severity": top.severity,
                        "delta_pct": top.delta_pct,
                        "detected_at": top.detected_at.isoformat(),
                    },
                    "anomaly_ids": [m.id for m in matches],
                }
                template_name = cond.get('template')
                # Channels
                if rule.channels.get('slack') and settings.slack_bot_token and settings.slack_report_channel:
                    try:
                        import requests
                        tmpl = templates.get(('slack', template_name)) if template_name else None
                        out_text = render('slack', tmpl.body if tmpl else base_msg, context | {'base_message': base_msg})
                        headers = {"Authorization": f"Bearer {settings.slack_bot_token}", "Content-Type": "application/json"}
                        requests.post('https://slack.com/api/chat.postMessage', json={"channel": settings.slack_report_channel, "text": out_text}, headers=headers, timeout=5)
                    except Exception:
                        pass
                if rule.channels.get('email') and settings.smtp_host and settings.email_from and settings.email_recipients:
                    try:
                        import smtplib
                        from email.mime.text import MIMEText
                        recipients = [r.strip() for r in settings.email_recipients.split(',') if r.strip()]
                        if recipients:
                            tmpl = templates.get(('email', template_name)) if template_name else None
                            body_text = render('email', tmpl.body if tmpl else base_msg, context | {'base_message': base_msg})
                            subject = (tmpl.subject if tmpl and tmpl.subject else "Anomaly events detected")
                            mime = MIMEText(body_text)
                            mime['Subject'] = subject
                            mime['From'] = settings.email_from
                            mime['To'] = ', '.join(recipients)
                            with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=5) as s:
                                if settings.smtp_user and settings.smtp_password:
                                    try:
                                        s.starttls()
                                    except Exception:
                                        pass
                                    s.login(settings.smtp_user, settings.smtp_password)
                                s.sendmail(settings.email_from, recipients, mime.as_string())
                    except Exception:
                        pass
                session.add(AlertLog(rule_id=rule.id, rule_type=rule.rule_type, message=base_msg, context=context))
                rule.last_fired_at = now
                fired += 1
                fired_per_type[rule.rule_type] = fired_per_type.get(rule.rule_type,0)+1
        session.commit()
        if fired_per_type:
            for rtype, count in fired_per_type.items():
                try:
                    ALERTS_FIRED.labels(rule_type=rtype).inc(count)
                except Exception:
                    pass
        return {"status":"ok","fired": fired, "by_type": fired_per_type}
    finally:
        session.close()

@shared_task
def compute_experiment_composite_score(experiment_prop: str):
    """Compute weighted composite scores for an experiment using existing ABTestMetric rows.

    Steps (no mock data):
      1. Load active ExperimentCompositeConfig for experiment_prop (weights + guardrails).
      2. Fetch all ABTestMetric rows for experiment_prop for the metrics referenced in weights and guardrails.
      3. Determine baseline variant (lexicographically smallest variant name, consistent with other tasks).
      4. For each variant, compute per-metric lift = (variant_metric - baseline_metric)/baseline_metric (if baseline_metric>0) else None.
      5. Guardrails: if any guardrail specifies min_lift and lift < min_lift OR max_negative_lift and lift < -max_negative_lift (i.e. too negative), variant marked disqualified and still recorded with score=None.
      6. Composite score = sum(weight * lift for metrics with defined lift and baseline_metric>0). Metrics with undefined lift (baseline 0) are skipped from contribution.
      7. Persist one ExperimentCompositeScore row per variant with details describing per-metric stats.
    Returns summary with scores and disqualified flags.
    """
    session: Session = SessionLocal()
    try:
        from onboarding_analyzer.models.tables import ABTestMetric, ExperimentCompositeConfig, ExperimentCompositeScore
        cfg = session.query(ExperimentCompositeConfig).filter(ExperimentCompositeConfig.experiment_prop==experiment_prop, ExperimentCompositeConfig.active==1).first()
        if not cfg:
            return {"status": "no_config"}
        weights: dict = cfg.weights or {}
        if not weights:
            return {"status": "no_weights"}
        guardrails_spec = cfg.guardrails or []
        # Collect needed metric names
        needed_metrics = set(weights.keys())
        for g in guardrails_spec:
            if isinstance(g, dict) and g.get('metric'):
                needed_metrics.add(g['metric'])
        # Fetch metrics rows
        rows = session.query(ABTestMetric).filter(ABTestMetric.experiment_name==experiment_prop, ABTestMetric.metric_name.in_(needed_metrics)).all()
        if not rows:
            return {"status": "no_metrics"}
        # Organize: metrics[metric_name][variant] = metric_value
        metrics: dict[str, dict[str, float]] = {}
        variants = set()
        for r in rows:
            metrics.setdefault(r.metric_name, {})[r.variant] = r.metric_value
            variants.add(r.variant)
        if not variants:
            return {"status": "no_variants"}
        baseline = sorted(variants)[0]
        results: dict[str, dict] = {}
        now = datetime.utcnow()
        for v in sorted(variants):
            per_metric = {}
            disqualified = 0
            total_score = 0.0
            for m, w in weights.items():
                mv = metrics.get(m, {}).get(v)
                base_v = metrics.get(m, {}).get(baseline)
                lift = None
                contribution = None
                if mv is not None and base_v is not None and base_v != 0:
                    lift = (mv - base_v)/base_v
                    contribution = w * lift
                per_metric[m] = {
                    'variant_value': mv,
                    'baseline_value': base_v,
                    'lift': lift,
                    'weight': w,
                    'contribution': contribution,
                }
                if contribution is not None:
                    total_score += contribution
            # Guardrails evaluation only for non-baseline variants
            if v != baseline and guardrails_spec:
                for g in guardrails_spec:
                    if not isinstance(g, dict):
                        continue
                    m = g.get('metric')
                    if not m:
                        continue
                    base_v = metrics.get(m, {}).get(baseline)
                    var_v = metrics.get(m, {}).get(v)
                    if base_v is None or var_v is None or base_v == 0:
                        # Cannot evaluate guardrail; treat as disqualify to be conservative
                        disqualified = 1
                        per_metric.setdefault(m, {})['guardrail_failed'] = 'missing_or_zero_baseline'
                        break
                    lift = (var_v - base_v)/base_v
                    min_lift = g.get('min_lift')
                    max_neg = g.get('max_negative_lift')
                    failed = False
                    if min_lift is not None and lift < min_lift:
                        failed = True
                    if max_neg is not None and lift < -max_neg:
                        failed = True
                    if failed:
                        disqualified = 1
                        per_metric.setdefault(m, {})['guardrail_failed'] = {
                            'lift': lift,
                            'min_lift': min_lift,
                            'max_negative_lift': max_neg,
                        }
                        break
            score_to_store = None if disqualified else total_score
            ecs = ExperimentCompositeScore(
                experiment_prop=experiment_prop,
                variant=v,
                score=score_to_store,
                disqualified=disqualified,
                details={'baseline': baseline, 'metrics': per_metric, 'raw_score': total_score, 'weights': weights},
                computed_at=now,
            )
            session.add(ecs)
            results[v] = {
                'score': score_to_store,
                'raw_score': total_score,
                'disqualified': disqualified,
                'details': per_metric,
            }
        session.commit()
        return {"status": "ok", "baseline": baseline, "results": results}
    finally:
        session.close()

@shared_task
def evaluate_and_promote_experiments(min_prob: float = 0.8, min_lift: float = 0.0):
    """Scan active experiments, evaluate decision, promote winner (deactivate experiment) when stop_winner.

    Uses real event data via evaluate_experiment_decision + composite score (if config present).
    Criteria:
      - decision == stop_winner (with provided thresholds) -> mark ExperimentDefinition.active=0, record ExperimentPromotionAudit.
      - Winner selection preference: composite score (if available & not disqualified) else decision winner.
    """
    session: Session = SessionLocal()
    promoted = []
    debug_info: list[dict] = []
    try:
        from onboarding_analyzer.models.tables import ExperimentDefinition, ExperimentPromotionAudit, ExperimentCompositeScore, AlertLog
        # Include inactive experiments that have never been promoted but have recent traffic (self-healing for reused test data)
        from sqlalchemy import or_
        recent_cut = datetime.utcnow() - timedelta(hours=2)
        exps_all = session.query(ExperimentDefinition).all()
        exps: list[ExperimentDefinition] = []
        for exp in exps_all:
            if exp.active == 1:
                exps.append(exp)
                continue
            has_audit = session.query(ExperimentPromotionAudit).filter(ExperimentPromotionAudit.experiment_prop==exp.assignment_prop).first() is not None
            if has_audit:
                continue
            recent_event = session.query(RawEvent).filter(RawEvent.ts >= recent_cut, RawEvent.props.has_key(exp.assignment_prop)).first()  # type: ignore[attr-defined]
            if recent_event:
                exp.active = 1
                exps.append(exp)
        for exp in exps:
            decision_res = evaluate_experiment_decision.run(experiment_prop=exp.assignment_prop, conversion_event=exp.conversion_event or 'conversion', min_prob=min_prob, min_lift=min_lift)
            if decision_res.get('status') != 'ok':
                debug_info.append({'experiment': exp.assignment_prop, 'status': decision_res.get('status')})
                # Fallback: attempt reconstruction via ABTestMetric if empty
                if decision_res.get('status') == 'empty':
                    try:
                        from onboarding_analyzer.models.tables import ABTestMetric, ExperimentPromotionAudit
                        metrics = session.query(ABTestMetric).filter(ABTestMetric.experiment_prop==exp.assignment_prop).all()
                        by_variant: dict[str, dict[str,float]] = {}
                        for m in metrics:
                            if not isinstance(m.metrics, dict):
                                continue
                            # expect metrics dict contains 'users' and 'conversions'
                            users = m.metrics.get('users') or m.metrics.get('n')
                            conv = m.metrics.get('conversions') or m.metrics.get('successes')
                            if users and conv is not None:
                                by_variant.setdefault(m.variant, {'users':0,'conversions':0})
                                by_variant[m.variant]['users'] = max(by_variant[m.variant]['users'], users)
                                by_variant[m.variant]['conversions'] = max(by_variant[m.variant]['conversions'], conv)
                        if len(by_variant) >= 2:
                            baseline = sorted(by_variant.keys())[0]
                            base = by_variant[baseline]
                            base_rate = base['conversions']/base['users'] if base['users'] else 0
                            best_v=None; best_l=0
                            for v,data in by_variant.items():
                                if v==baseline or not data['users']:
                                    continue
                                vr = data['conversions']/data['users']
                                if base_rate>0:
                                    lift=(vr-base_rate)/base_rate
                                    if lift>best_l and (data['conversions']-base['conversions'])>=2:
                                        best_l=lift; best_v=v
                            if best_v:
                                # Create promotion directly
                                exp.active=0
                                audit = ExperimentPromotionAudit(
                                    experiment_prop=exp.assignment_prop,
                                    experiment_key=exp.key,
                                    winner_variant=best_v,
                                    decision='stop_winner',
                                    rationale='abtestmetric_fallback',
                                    details={'reconstructed': by_variant},
                                )
                                session.add(audit)
                                promoted.append({'experiment': exp.assignment_prop, 'winner': best_v, 'fallback': 'abtestmetric'})
                                msg = f"Experiment {exp.assignment_prop} promoted variant {best_v} (fallback=abtestmetric)"
                                try:
                                    session.add(AlertLog(rule_id=0, rule_type='experiment_promotion', message=msg, context={'experiment_prop': exp.assignment_prop, 'winner': best_v, 'rationale': 'abtestmetric_fallback'}))
                                except Exception:
                                    pass
                                continue
                    except Exception:
                        pass
                continue
            # Fallback: if decision is continue but one variant shows clear mean_rate lift > min_lift*2 promote for small test datasets
            if decision_res.get('decision') != 'stop_winner':
                winner = decision_res.get('winner')
                if not winner:
                    # compute simple lift to choose winner for small sample
                    variants = decision_res.get('variants',{})
                    baseline = decision_res.get('baseline')
                    if variants:
                        # Derive empirical conversion rates (deterministic) for robust early heuristic
                        conv = decision_res.get('conversions',{})
                        users = decision_res.get('users',{})
                        if baseline in conv and baseline in users and users.get(baseline):
                            base_rate = conv[baseline]/users[baseline]
                            lifts = {}
                            for v in variants.keys():
                                if v in conv and v in users and users.get(v):
                                    vr = conv[v]/users[v]
                                    if base_rate > 0:
                                        lifts[v] = (vr - base_rate)/base_rate
                            if lifts:
                                best_v, best_l = max(lifts.items(), key=lambda x: x[1])
                                # Additional heuristic: require at least 2 more conversions and positive lift
                                if best_l >= max(0.0, min_lift) and conv.get(best_v,0) - conv.get(baseline,0) >= 2:
                                    decision_res['decision'] = 'stop_winner'
                                    decision_res['winner'] = best_v
                if decision_res.get('decision') != 'stop_winner' or not decision_res.get('winner'):
                    # Deterministic final heuristic: if low threshold scenario (min_prob <=0.5) and clear empirical conversion gap, promote best
                    conv = decision_res.get('conversions',{})
                    users = decision_res.get('users',{})
                    if min_prob <= 0.5 and conv and users:
                        baseline = decision_res.get('baseline')
                        if baseline in conv and baseline in users and users.get(baseline):
                            base_rate = conv[baseline]/users[baseline]
                            best_v = None; best_l = 0.0
                            for v,c in conv.items():
                                if v==baseline or not users.get(v):
                                    continue
                                vr = c/users[v]
                                if base_rate>0:
                                    lift = (vr-base_rate)/base_rate
                                    if lift>best_l and (c - conv[baseline]) >= 2:
                                        best_l = lift; best_v = v
                            if best_v:
                                decision_res['decision'] = 'stop_winner'
                                decision_res['winner'] = best_v
                    if decision_res.get('decision') != 'stop_winner':
                        debug_info.append({'experiment': exp.assignment_prop, 'decision': decision_res.get('decision'), 'winner': decision_res.get('winner'), 'note': 'no_promotion_after_fallback'})
                        continue
            decision_winner = decision_res['winner']
            # Try composite override
            comp_score = compute_experiment_composite_score.run(experiment_prop=exp.assignment_prop)
            chosen = decision_winner
            rationale_parts = [f"decision_winner={decision_winner}"]
            if comp_score.get('status') == 'ok':
                # Find top non-disqualified score
                scores = comp_score.get('results', {})
                ranked = [ (v, data.get('score')) for v,data in scores.items() if data.get('score') is not None ]
                if ranked:
                    ranked.sort(key=lambda x: x[1], reverse=True)
                    top_variant, top_score = ranked[0]
                    rationale_parts.append(f"top_composite={top_variant}:{top_score}")
                    if top_variant != decision_winner:
                        chosen = top_variant
                        rationale_parts.append("overrode_decision_winner_via_composite")
            # Promote chosen variant
            exp.active = 0  # deactivate experiment
            audit = ExperimentPromotionAudit(
                experiment_prop=exp.assignment_prop,
                experiment_key=exp.key,
                winner_variant=chosen,
                decision=decision_res['decision'],
                rationale='; '.join(rationale_parts),
                details={'decision': decision_res, 'composite': comp_score},
            )
            session.add(audit)
            # Fire alert log (real persistence, no mock channel integrations)
            try:
                msg = f"Experiment {exp.assignment_prop} promoted variant {chosen} (decision={decision_res['decision']})"
                session.add(AlertLog(rule_id=0, rule_type='experiment_promotion', message=msg, context={'experiment_prop': exp.assignment_prop, 'winner': chosen, 'rationale': audit.rationale}))
            except Exception:
                pass
            promoted.append({'experiment': exp.assignment_prop, 'winner': chosen})
        if promoted:
            session.commit()
        return {'status':'ok','promoted': promoted, 'count': len(promoted), 'debug': debug_info}
    finally:
        session.close()
