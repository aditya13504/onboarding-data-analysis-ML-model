from celery import shared_task
from sqlalchemy.orm import Session
from sqlalchemy import select
from sklearn.cluster import KMeans
import numpy as np
from onboarding_analyzer.infrastructure.db import SessionLocal
from onboarding_analyzer.models.tables import RawEvent, FrictionCluster, ModelVersion, ModelArtifact
from onboarding_analyzer.tasks.analytics import _derive_ordered_steps
from sklearn.metrics import silhouette_score, davies_bouldin_score, calinski_harabasz_score
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import NMF


@shared_task
def refresh_clusters(k: int = 4):
    session: Session = SessionLocal()
    try:
        # Simple feature: number of steps reached, time span
        user_sessions = {}
        q = select(RawEvent.user_id, RawEvent.session_id, RawEvent.event_name, RawEvent.ts).order_by(RawEvent.user_id, RawEvent.session_id, RawEvent.ts)
        for user_id, session_id, event_name, ts in session.execute(q):
            key = (user_id, session_id)
            rec = user_sessions.setdefault(key, {"events": [], "first": ts, "last": ts})
            rec["events"].append(event_name)
            rec["last"] = ts
        features = []
        meta = []
        # derive total possible steps from observed ordering
        steps_all = _derive_ordered_steps(session)
        total_steps = max(len(steps_all), 1)
        for (user_id, session_id), rec in user_sessions.items():
            steps_reached = len(set(rec["events"]))
            duration = (rec["last"] - rec["first"]).total_seconds() or 1.0
            completion_ratio = steps_reached / total_steps
            features.append([steps_reached, duration, completion_ratio])
            meta.append((user_id, session_id, rec["events"]))
        if not features:
            return {"status": "no_data"}
        X = np.array(features)
        k = min(k, len(X))
        model = KMeans(n_clusters=k, n_init=5)
        labels = model.fit_predict(X)
        # Cluster quality metrics
        sil = 0.0
        dbi = None
        ch = None
        if k > 1 and len(set(labels)) > 1:
            try:
                sil = float(silhouette_score(X, labels))
            except Exception:
                pass
            try:
                dbi = float(davies_bouldin_score(X, labels))
            except Exception:
                dbi = None
            try:
                ch = float(calinski_harabasz_score(X, labels))
            except Exception:
                ch = None
        # Clear old clusters
        session.query(FrictionCluster).delete()
        # Prepare simple event sequence bag-of-words for topic modeling (optional)
        sequences = [" ".join(rec[2]) for rec in meta]
        topics_per_cluster: dict[int, list[str]] = {}
        if sequences and len(set(labels)) > 1:
            try:
                vec = TfidfVectorizer(max_features=200, ngram_range=(1,2))
                X_txt = vec.fit_transform(sequences)
                nmf = NMF(n_components=min(5, k), init='nndsvd', max_iter=200)
                W = nmf.fit_transform(X_txt)
                H = nmf.components_
                feature_names = vec.get_feature_names_out()
                # Assign top terms per cluster by averaging W rows for sequences in that cluster
                for cluster_id in range(k):
                    idxs = np.where(labels == cluster_id)[0]
                    if len(idxs) == 0:
                        continue
                    avg_topic = np.mean(W[idxs], axis=0)
                    top_topic_idx = int(np.argmax(avg_topic)) if avg_topic.size else 0
                    top_terms = []
                    if H.shape[0] > top_topic_idx:
                        comp = H[top_topic_idx]
                        top_inds = np.argsort(comp)[-5:][::-1]
                        top_terms = [feature_names[i] for i in top_inds]
                    topics_per_cluster[cluster_id] = top_terms
            except Exception:
                pass
        for cluster_id in range(k):
            idxs = np.where(labels == cluster_id)[0]
            size = len(idxs)
            if size == 0:
                continue
            subset = X[idxs]
            avg_steps = float(np.mean(subset[:, 0]))
            avg_duration = float(np.mean(subset[:, 1]))
            avg_completion = float(np.mean(subset[:, 2]))
            drop_off_users = int(np.sum(subset[:, 2] < 1.0))
            impact_score = drop_off_users * (1 - avg_completion)
            cluster = FrictionCluster(
                label=f"Cluster {cluster_id}",
                size=size,
                drop_off_users=drop_off_users,
                impact_score=impact_score,
                features_summary={
                    "avg_steps": avg_steps,
                    "avg_duration": avg_duration,
                    "avg_completion": avg_completion,
                    "topic_terms": topics_per_cluster.get(cluster_id),
                },
                model_version="v0",
            )
            session.add(cluster)
        session.commit()
        # register model version (simple increment by timestamp)
        from datetime import datetime
        version = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        mv = ModelVersion(model_name="cluster", version=version)
        session.add(mv)
        # Persist lightweight model artifact (centers & inertia)
        # Fetch last artifact for stability comparison
        prev_artifact = session.query(ModelArtifact).filter(ModelArtifact.model_name=="cluster").order_by(ModelArtifact.created_at.desc()).first()
        prev_inertia = (prev_artifact.artifact or {}).get("inertia") if prev_artifact else None
        inertia_delta = None
        if prev_inertia is not None:
            try:
                inertia_delta = float(model.inertia_ - float(prev_inertia))
            except Exception:
                inertia_delta = None
        artifact_payload = {
            "centers": model.cluster_centers_.tolist(),
            "inertia": float(model.inertia_),
            "k": int(k),
            "features": ["steps_reached", "duration", "completion_ratio"],
            "silhouette": sil,
            "davies_bouldin": dbi,
            "calinski_harabasz": ch,
            "inertia_delta": inertia_delta,
        }
        session.add(ModelArtifact(model_name="cluster", version=version, artifact=artifact_payload))
        # Auto-promotion: if silhouette improves over last promoted or no promoted exists
        prev = session.query(ModelVersion).filter(ModelVersion.model_name=="cluster", ModelVersion.promoted==1).order_by(ModelVersion.created_at.desc()).first()
        promote = False
        if not prev:
            promote = True
        else:
            prev_art = session.query(ModelArtifact).filter(ModelArtifact.model_name=="cluster", ModelArtifact.version==prev.version).first()
            prev_sil = (prev_art.artifact or {}).get("silhouette") if prev_art else None
            if prev_sil is None or sil >= (prev_sil + 0.01):  # minimal improvement threshold
                promote = True
        if promote:
            # demote current promoted
            session.query(ModelVersion).filter(ModelVersion.model_name=="cluster", ModelVersion.promoted==1).update({ModelVersion.promoted: 0})
            mv.promoted = 1
        session.commit()
        return {"status": "ok", "clusters": k, "version": version, "silhouette": sil, "promoted": bool(mv.promoted)}
    finally:
        session.close()
