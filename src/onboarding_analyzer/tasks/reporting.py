from celery import shared_task
from sqlalchemy.orm import Session
from onboarding_analyzer.infrastructure.db import SessionLocal
from onboarding_analyzer.models.tables import FunnelMetric, Insight, ReportLog, FrictionCluster
from onboarding_analyzer.config import get_settings
from onboarding_analyzer.utils.emailing import send_email
from slack_sdk import WebClient
from notion_client import Client as NotionClient
import io
import matplotlib.pyplot as plt
from datetime import datetime


@shared_task
def compose_weekly_report() -> dict:
    """Compose and optionally publish a weekly operational report.

    Contents:
      - Funnel metrics with per-step conversion and an ASCII sparkline.
      - Top insights (ordered by priority) and friction cluster exemplars.
      - Insight lifecycle deltas for the past 7 days.
    Side-effects (best-effort, non-fatal):
      - Slack message (and chart upload) if Slack configured.
      - Notion page creation if Notion configured.
    Returns structured payload without placeholder values.
    """
    session: Session = SessionLocal()
    try:
        funnel = session.query(FunnelMetric).order_by(FunnelMetric.step_order).all()
        insights = session.query(Insight).order_by(Insight.priority).all()
        clusters = session.query(FrictionCluster).order_by(FrictionCluster.impact_score.desc()).limit(5).all()
        new_cnt = sum(1 for i in insights if (datetime.utcnow() - i.created_at).days < 7)
        progressed_cnt = sum(1 for i in insights if i.status in ("accepted","shipped") and i.accepted_at and (datetime.utcnow()-i.accepted_at).days < 7)
        shipped_cnt = sum(1 for i in insights if i.status=="shipped" and i.shipped_at and (datetime.utcnow()-i.shipped_at).days < 7)
        reopened_cnt = sum(1 for i in insights if i.status in ("accepted","open") and i.shipped_at and (datetime.utcnow()-i.shipped_at).days < 7)
        lines: list[str] = []
        lines.append(f"Weekly Onboarding Report - {datetime.utcnow():%Y-%m-%d}")
        lines.append(f"Insight Changes: new={new_cnt} progressed={progressed_cnt} shipped={shipped_cnt} reopened={reopened_cnt}")
        lines.append("Funnel Metrics:")
        step_labels: list[str] = []
        cr_values: list[float] = []
        def spark(values: list[float]) -> str:
            if not values:
                return ''
            blocks = '▁▂▃▄▅▆▇█'
            mn, mx = min(values), max(values)
            rng = (mx - mn) or 1.0
            return ''.join(blocks[int((v - mn)/rng * (len(blocks)-1))] for v in values)
        for m in funnel:
            lines.append(f"  {m.step_order+1}. {m.step_name}: entered={m.users_entered} converted={m.users_converted} drop_off={m.drop_off} CR={m.conversion_rate:.2%}")
            step_labels.append(m.step_name)
            cr_values.append(m.conversion_rate)
        if cr_values:
            lines.append(f"  Conversion sparkline: {spark(cr_values)}")
        lines.append("")
        lines.append("Top Insights:")
        for ins in insights:
            lines.append(f"  ({ins.priority}) {ins.title} -> {ins.recommendation} [impact={ins.impact_score:.2f}]")
        if clusters:
            lines.append("")
            lines.append("Cluster Exemplars:")
            for c in clusters:
                fs = c.features_summary or {}
                lines.append(f"  {c.label}: size={c.size} impact={c.impact_score:.2f} steps={fs.get('avg_steps')} duration={fs.get('avg_duration')} completion={fs.get('avg_completion')} topics={','.join(fs.get('topic_terms') or []) if fs.get('topic_terms') else '-'}")
        report_text = "\n".join(lines)
        settings = get_settings()
        publish_targets: dict[str,str] = {}
        chart_bytes: bytes | None = None
        if step_labels:
            fig, ax = plt.subplots(figsize=(6, 3))
            ax.bar(step_labels, cr_values, color="#4B8BBE")
            ax.set_ylabel("Conversion Rate")
            ax.set_title("Funnel Conversion")
            ax.set_ylim(0, 1)
            fig.tight_layout()
            buf = io.BytesIO()
            fig.savefig(buf, format="png")
            plt.close(fig)
            buf.seek(0)
            chart_bytes = buf.read()
        if settings.slack_bot_token and settings.slack_report_channel:
            try:
                client = WebClient(token=settings.slack_bot_token)
                if chart_bytes:
                    upload = client.files_upload_v2(channel=settings.slack_report_channel, filename="funnel.png", file=chart_bytes, initial_comment=report_text[:1500])
                    publish_targets["slack_file_id"] = upload.get("file", {}).get("id")
                else:
                    resp = client.chat_postMessage(channel=settings.slack_report_channel, text=report_text)
                    publish_targets["slack_ts"] = resp.get("ts")
            except Exception as e:  # noqa
                publish_targets["slack_error"] = str(e)
        if settings.notion_api_key and settings.notion_database_id:
            try:
                notion = NotionClient(auth=settings.notion_api_key)
                notion.pages.create(
                    parent={"database_id": settings.notion_database_id},
                    properties={
                        "Name": {"title": [{"text": {"content": f"Weekly Report {datetime.utcnow():%Y-%m-%d}"}}]},
                    },
                    children=[{"object": "block", "type": "paragraph", "paragraph": {"rich_text": [{"type": "text", "text": {"content": report_text[:1900]}}]}}],
                )
                publish_targets["notion"] = "ok"
            except Exception as e:  # noqa
                publish_targets["notion_error"] = str(e)
        return {"status": "ok", "report": report_text, "new_insights": new_cnt, "progressed_insights": progressed_cnt, "shipped_insights": shipped_cnt, "reopened_insights": reopened_cnt, **publish_targets}
    finally:
        session.close()


@shared_task
def publish_weekly_report():
    """Generate, persist, and publish the weekly report with logging."""
    result = compose_weekly_report()
    session: Session = SessionLocal()
    try:
        # Email digest (HTML) if configured
        settings = get_settings()
        if settings.smtp_host and settings.email_from and settings.email_recipients:
            recips = [r.strip() for r in (settings.email_recipients or '').split(',') if r.strip()]
            html = f"""
            <html><body>
            <h2>Weekly Onboarding Report</h2>
            <pre style='font-family:monospace'>{result.get('report','').replace('&','&amp;').replace('<','&lt;')}</pre>
            </body></html>
            """
            send_email("Weekly Onboarding Report", html, recips, settings.email_from, settings.smtp_host, settings.smtp_port, settings.smtp_user, settings.smtp_password, [])
        rl = ReportLog(
            report_type="weekly",
            content=result.get("report",""),
            insights_count=result.get("insights_count", 0) if isinstance(result.get("insights_count"), int) else 0,
            funnel_steps=result.get("funnel_steps", 0) if isinstance(result.get("funnel_steps"), int) else 0,
            published_slack=1 if any(k.startswith("slack_") for k in result.keys()) else 0,
            published_notion=1 if "notion" in result else 0,
            new_insights=result.get("new_insights",0),
            progressed_insights=result.get("progressed_insights",0),
            shipped_insights=result.get("shipped_insights",0),
            reopened_insights=result.get("reopened_insights",0),
        )
        session.add(rl)
        session.commit()
        return {"status": "ok", "report_id": rl.id}
    finally:
        session.close()
