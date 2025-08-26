"""Friction clustering pipeline (no mock data).

Extract recent session summaries + event features, build feature matrix, run clustering (k-means fallback)
and persist FrictionCluster rows with size, impact score, feature summaries.

Impact score heuristic: cluster_dropoff_users / total_dropoff_users * (avg_completion_gap)
"""
from __future__ import annotations
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
from statistics import mean, pstdev
from collections import defaultdict
from sqlalchemy import func, and_
from onboarding_analyzer.infrastructure.db import SessionLocal
from onboarding_analyzer.models.tables import SessionSummary, FrictionCluster, RawEvent, ModelVersion, ModelArtifact

try:
	from sklearn.cluster import KMeans
	_sk_ok = True
except Exception:
	_sk_ok = False

def _base_session_features(row: SessionSummary) -> list[float]:
	"""Baseline features from summary row."""
	return [
		float(row.duration_sec or 0.0),
		float(row.steps_count or 0),
		float(row.completion_ratio or 0.0),
	]

def _advanced_session_features(events: List[RawEvent]) -> Tuple[float, float, float, int]:
	"""Compute advanced temporal & pattern features from ordered raw events.

	Returns tuple: (inter_event_mean, inter_event_std, first_step_share, early_abandon_flag)
	Where:
	  inter_event_mean/std: Mean & std dev (population) of time deltas between consecutive events (seconds).
	  first_step_share: Fraction of session duration accounted for by time until second event (proxy for early friction); 0 if <2 events or duration=0.
	  early_abandon_flag: 1 if user produced only 1-2 events and no completion event (assumed if <3 events), else 0.
	"""
	if not events:
		return 0.0, 0.0, 0.0, 0
	# Ensure ordered
	events_sorted = sorted(events, key=lambda e: e.ts)
	if len(events_sorted) < 2:
		early_flag = 1
		return 0.0, 0.0, 0.0, early_flag
	deltas: List[float] = []
	for a, b in zip(events_sorted, events_sorted[1:]):
		try:
			deltas.append((b.ts - a.ts).total_seconds())
		except Exception:
			pass
	inter_mean = float(mean(deltas)) if deltas else 0.0
	inter_std = float(pstdev(deltas)) if len(deltas) > 1 else 0.0
	duration_total = max((events_sorted[-1].ts - events_sorted[0].ts).total_seconds(), 0.0)
	first_step_share = 0.0
	if duration_total > 0 and len(events_sorted) >= 2:
		try:
			first_step_share = ((events_sorted[1].ts - events_sorted[0].ts).total_seconds()) / duration_total
		except Exception:
			first_step_share = 0.0
	early_abandon_flag = 1 if len(events_sorted) <= 2 else 0
	return inter_mean, inter_std, first_step_share, early_abandon_flag

def run_friction_clustering(lookback_hours: int = 24, k: int = 5, project: str | None = None):
	"""Advanced friction clustering with enriched temporal features and artifact persistence.

	Features per session:
	  0 duration_sec
	  1 steps_count
	  2 completion_ratio
	  3 inter_event_mean
	  4 inter_event_std
	  5 first_step_share
	  6 early_abandon_flag
	"""
	s = SessionLocal()
	try:
		since = datetime.utcnow() - timedelta(hours=lookback_hours)
		q = s.query(SessionSummary).filter(SessionSummary.start_ts >= since)
		if project:
			q = q.filter(SessionSummary.project == project)
		sessions: List[SessionSummary] = q.all()
		if not sessions:
			return {"status": "no_sessions"}
		# Bulk fetch raw events for these sessions to compute advanced features
		# Build mapping key -> events list
		pairs = {(sess.user_id, sess.session_id) for sess in sessions}
		# Query events in single pass (filter by ts >= since AND project if provided)
		ev_q = s.query(RawEvent).filter(RawEvent.ts >= since)
		if project:
			ev_q = ev_q.filter(RawEvent.project == project)
		events_all: List[RawEvent] = ev_q.all()
		events_map: Dict[Tuple[str, str], List[RawEvent]] = defaultdict(list)
		for ev in events_all:
			key = (ev.user_id, ev.session_id)
			if key in pairs:  # restrict to relevant sessions
				events_map[key].append(ev)
		feature_rows: List[List[float]] = []
		advanced_pieces: List[Tuple[float, float, float, int]] = []
		for sess in sessions:
			adv = _advanced_session_features(events_map.get((sess.user_id, sess.session_id), []))
			advanced_pieces.append(adv)
			feature_rows.append(_base_session_features(sess) + list(adv))
		if not any(any(v for v in row) for row in feature_rows):
			return {"status": "no_variance"}
		n_clusters = min(k, max(2, len(feature_rows)//5)) if len(feature_rows) >= 10 else min(2, k)
		if n_clusters < 2:
			n_clusters = 2 if len(feature_rows) >= 2 else 1
		labels: List[int]
		if _sk_ok and n_clusters > 1:
			model = KMeans(n_clusters=n_clusters, n_init=10, random_state=42)
			labels = model.fit_predict(feature_rows).tolist()
			centers = model.cluster_centers_.tolist()
			inertia = float(model.inertia_)
		else:
			centers = feature_rows[:n_clusters]
			labels = []
			for row in feature_rows:
				if not centers:
					centers = [row]
				# assign to closest center (euclidean)
				dists = [sum((a-b)**2 for a, b in zip(row, c)) for c in centers]
				labels.append(dists.index(min(dists)))
			inertia = 0.0
		# Aggregate clusters
		cluster_map: Dict[int, Dict] = {}
		total_sessions = len(sessions)
		for sess, lab, adv in zip(sessions, labels, advanced_pieces):
			c = cluster_map.setdefault(lab, {
				"sessions": [],
				"durations": [],
				"steps": [],
				"completions": [],
				"inter_means": [],
				"inter_stds": [],
				"first_shares": [],
				"early_flags": []
			})
			c["sessions"].append(sess)
			c["durations"].append(sess.duration_sec)
			c["steps"].append(sess.steps_count)
			c["completions"].append(sess.completion_ratio)
			c["inter_means"].append(adv[0])
			c["inter_stds"].append(adv[1])
			c["first_shares"].append(adv[2])
			c["early_flags"].append(adv[3])
		# Clear existing clusters for project to avoid mixing versions
		try:
			if project:
				s.query(FrictionCluster).filter(FrictionCluster.project == project).delete()
			else:
				s.query(FrictionCluster).filter(FrictionCluster.project.is_(None)).delete()
			s.commit()
		except Exception:
			s.rollback()
		feature_names = [
			"duration_sec", "steps_count", "completion_ratio",
			"inter_event_mean", "inter_event_std", "first_step_share", "early_abandon_flag"
		]
		# Compute & persist clusters
		for lab, data in cluster_map.items():
			size = len(data['sessions'])
			if size == 0:
				continue
			drop_users = sum(1 for sess in data['sessions'] if (sess.completion_ratio or 0) < 1.0)
			avg_comp = mean([sess.completion_ratio or 0 for sess in data['sessions']]) if data['sessions'] else 0.0
			early_rate = mean(data['early_flags']) if data['early_flags'] else 0.0
			impact = (drop_users/total_sessions) * (1-avg_comp) * (1 + early_rate*0.5)
			fc = FrictionCluster(
				label=f"cluster_{lab}",
				size=size,
				drop_off_users=drop_users,
				impact_score=impact,
				features_summary={
					'avg_duration': mean([d or 0 for d in data['durations']]) if data['durations'] else 0,
					'avg_steps': mean([st or 0 for st in data['steps']]) if data['steps'] else 0,
					'avg_completion': avg_comp,
					'avg_inter_event_mean': mean(data['inter_means']) if data['inter_means'] else 0.0,
					'avg_inter_event_std': mean(data['inter_stds']) if data['inter_stds'] else 0.0,
					'avg_first_step_share': mean(data['first_shares']) if data['first_shares'] else 0.0,
					'early_abandon_rate': early_rate,
				},
				model_version='v2',
				project=project,
			)
			s.add(fc)
		# Create model version + artifact
		version = datetime.utcnow().strftime("%Y%m%d%H%M%S")
		mv = ModelVersion(model_name="friction_cluster", version=version)
		s.add(mv)
		artifact_payload = {
			'feature_names': feature_names,
			'centers': centers,
			'k': len(cluster_map),
			'inertia': inertia,
			'lookback_hours': lookback_hours,
			'project': project,
		}
		s.add(ModelArtifact(model_name="friction_cluster", version=version, artifact=artifact_payload))
		# Auto-promotion if none promoted or more clusters (proxy for granularity)
		try:
			prev_promoted = s.query(ModelVersion).filter(ModelVersion.model_name == "friction_cluster", ModelVersion.promoted == 1).order_by(ModelVersion.created_at.desc()).first()
			promote = False
			if not prev_promoted:
				promote = True
			else:
				prev_art = s.query(ModelArtifact).filter(ModelArtifact.model_name=="friction_cluster", ModelArtifact.version==prev_promoted.version).first()
				if prev_art and (artifact_payload.get('k',0) >= (prev_art.artifact or {}).get('k',0)):
					promote = True
			if promote:
				s.query(ModelVersion).filter(ModelVersion.model_name=="friction_cluster", ModelVersion.promoted==1).update({ModelVersion.promoted: 0})
				mv.promoted = 1
		except Exception:
			pass
		s.commit()
		return {"status": "ok", "clusters": len(cluster_map), "version": version, "promoted": bool(getattr(mv, 'promoted', 0)), "project": project}
	finally:
		s.close()
