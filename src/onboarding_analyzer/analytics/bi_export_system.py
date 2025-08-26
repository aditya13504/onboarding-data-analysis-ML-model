"""
Business Intelligence Export and Notification System

Comprehensive export capabilities and notification system for BI reports.
Supports multiple export formats, email notifications, and integration with external systems.
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Union, Callable
from dataclasses import dataclass
from pathlib import Path
import json
import base64
import io
import zipfile
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import smtplib
import ssl

# File handling
import pandas as pd
try:
    from reportlab.lib.pagesizes import letter, A4
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import inch
    from reportlab.lib import colors
    HAS_REPORTLAB = True
except ImportError:
    HAS_REPORTLAB = False

# Monitoring
from prometheus_client import Counter, Histogram

# Initialize metrics
reports_exported = Counter('bi_reports_exported_total', 'Reports exported', ['format'])
notifications_sent = Counter('bi_notifications_sent_total', 'Notifications sent', ['type'])
export_failures = Counter('bi_export_failures_total', 'Export failures', ['format'])
export_duration = Histogram('bi_export_duration_seconds', 'Export operation duration', ['format'])

logger = logging.getLogger(__name__)

@dataclass
class ExportConfig:
    """Configuration for report exports."""
    export_id: str
    export_format: str  # 'pdf', 'excel', 'csv', 'json', 'html'
    output_path: Optional[str] = None
    include_visualizations: bool = True
    include_raw_data: bool = False
    compression: bool = False
    password_protection: bool = False
    password: Optional[str] = None
    metadata: Dict[str, Any] = None

@dataclass
class NotificationConfig:
    """Configuration for notifications."""
    notification_id: str
    notification_type: str  # 'email', 'webhook', 'slack', 'teams'
    recipients: List[str]
    subject: str
    message: str
    attachments: List[str] = None
    priority: str = 'normal'  # 'low', 'normal', 'high', 'urgent'
    retry_attempts: int = 3
    metadata: Dict[str, Any] = None

@dataclass
class EmailConfig:
    """Email server configuration."""
    smtp_server: str
    smtp_port: int
    username: str
    password: str
    use_tls: bool = True
    use_ssl: bool = False
    sender_email: str = None
    sender_name: str = "BI System"

class ReportExporter:
    """Advanced report export system with multiple format support."""
    
    def __init__(self, base_export_path: str = "./exports"):
        self.base_export_path = Path(base_export_path)
        self.base_export_path.mkdir(exist_ok=True)
        
        # Export handlers
        self.export_handlers = {
            'json': self._export_json,
            'csv': self._export_csv,
            'excel': self._export_excel,
            'html': self._export_html,
            'pdf': self._export_pdf
        }
        
    async def export_report(self, report_data: Dict[str, Any],
                          export_config: ExportConfig) -> Dict[str, Any]:
        """Export report in specified format."""
        try:
            with export_duration.labels(format=export_config.export_format).time():
                # Validate format
                if export_config.export_format not in self.export_handlers:
                    raise ValueError(f"Unsupported export format: {export_config.export_format}")
                
                # Generate output path
                if not export_config.output_path:
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    filename = f"report_{export_config.export_id}_{timestamp}.{export_config.export_format}"
                    output_path = self.base_export_path / filename
                else:
                    output_path = Path(export_config.output_path)
                
                # Export using appropriate handler
                handler = self.export_handlers[export_config.export_format]
                file_path = await handler(report_data, output_path, export_config)
                
                # Apply post-processing
                final_path = await self._post_process_export(file_path, export_config)
                
                # Update metrics
                reports_exported.labels(format=export_config.export_format).inc()
                
                export_result = {
                    'export_id': export_config.export_id,
                    'export_format': export_config.export_format,
                    'file_path': str(final_path),
                    'file_size': final_path.stat().st_size,
                    'export_timestamp': datetime.now().isoformat(),
                    'success': True
                }
                
                logger.info(f"Report exported successfully: {final_path}")
                return export_result
                
        except Exception as e:
            export_failures.labels(format=export_config.export_format).inc()
            logger.error(f"Report export failed: {e}")
            
            return {
                'export_id': export_config.export_id,
                'export_format': export_config.export_format,
                'error': str(e),
                'export_timestamp': datetime.now().isoformat(),
                'success': False
            }
    
    async def _export_json(self, report_data: Dict[str, Any], 
                          output_path: Path, config: ExportConfig) -> Path:
        """Export report as JSON."""
        try:
            # Prepare data for JSON export
            export_data = {
                'report_metadata': report_data.get('report_metadata', {}),
                'kpi_summary': self._serialize_kpi_data(report_data.get('kpi_summary', {})),
                'performance_summary': report_data.get('performance_summary', {}),
                'trend_analysis': report_data.get('trend_analysis', {}),
                'export_timestamp': datetime.now().isoformat()
            }
            
            # Include visualizations if requested
            if config.include_visualizations and 'visualizations' in report_data:
                export_data['visualizations'] = report_data['visualizations']
            
            # Include raw data if requested
            if config.include_raw_data and 'raw_data' in report_data:
                export_data['raw_data'] = report_data['raw_data']
            
            # Write JSON file
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, indent=2, ensure_ascii=False, default=str)
            
            return output_path
            
        except Exception as e:
            logger.error(f"JSON export failed: {e}")
            raise
    
    async def _export_csv(self, report_data: Dict[str, Any], 
                         output_path: Path, config: ExportConfig) -> Path:
        """Export report as CSV."""
        try:
            # Extract KPI data for CSV
            kpi_data = []
            kpi_summary = report_data.get('kpi_summary', {})
            
            for kpi_id, kpi_result in kpi_summary.items():
                kpi_data.append({
                    'KPI_ID': kpi_id,
                    'Value': kpi_result.get('value', 0),
                    'Status': kpi_result.get('status', 'unknown'),
                    'Trend': kpi_result.get('trend', 'stable'),
                    'Change_Percentage': kpi_result.get('change_percentage', 0),
                    'Target_Achievement': kpi_result.get('target_achievement', 0),
                    'Timestamp': datetime.now().isoformat()
                })
            
            # Create DataFrame and save
            df = pd.DataFrame(kpi_data)
            df.to_csv(output_path, index=False, encoding='utf-8')
            
            return output_path
            
        except Exception as e:
            logger.error(f"CSV export failed: {e}")
            raise
    
    async def _export_excel(self, report_data: Dict[str, Any], 
                           output_path: Path, config: ExportConfig) -> Path:
        """Export report as Excel with multiple sheets."""
        try:
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                # KPI Summary sheet
                kpi_data = self._prepare_kpi_dataframe(report_data.get('kpi_summary', {}))
                if not kpi_data.empty:
                    kpi_data.to_excel(writer, sheet_name='KPI_Summary', index=False)
                
                # Performance Summary sheet
                performance_data = report_data.get('performance_summary', {})
                if performance_data:
                    perf_df = pd.DataFrame([performance_data])
                    perf_df.to_excel(writer, sheet_name='Performance_Summary', index=False)
                
                # Trend Analysis sheet
                trend_data = report_data.get('trend_analysis', {})
                if trend_data:
                    trend_df = pd.DataFrame([trend_data])
                    trend_df.to_excel(writer, sheet_name='Trend_Analysis', index=False)
                
                # Metadata sheet
                metadata = report_data.get('report_metadata', {})
                if metadata:
                    meta_df = pd.DataFrame([metadata])
                    meta_df.to_excel(writer, sheet_name='Metadata', index=False)
                
                # Raw data sheet (if requested)
                if config.include_raw_data and 'user_metrics' in report_data:
                    user_metrics = report_data['user_metrics']
                    if user_metrics:
                        user_df = pd.DataFrame([user_metrics])
                        user_df.to_excel(writer, sheet_name='User_Metrics', index=False)
            
            return output_path
            
        except Exception as e:
            logger.error(f"Excel export failed: {e}")
            raise
    
    async def _export_html(self, report_data: Dict[str, Any], 
                          output_path: Path, config: ExportConfig) -> Path:
        """Export report as HTML."""
        try:
            # Generate HTML content
            html_content = self._generate_html_report(report_data, config)
            
            # Write HTML file
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            return output_path
            
        except Exception as e:
            logger.error(f"HTML export failed: {e}")
            raise
    
    async def _export_pdf(self, report_data: Dict[str, Any], 
                         output_path: Path, config: ExportConfig) -> Path:
        """Export report as PDF."""
        try:
            if not HAS_REPORTLAB:
                raise ImportError("ReportLab is required for PDF export")
            
            # Create PDF document
            doc = SimpleDocTemplate(str(output_path), pagesize=A4)
            story = []
            styles = getSampleStyleSheet()
            
            # Title
            title_style = ParagraphStyle(
                'CustomTitle',
                parent=styles['Heading1'],
                fontSize=24,
                textColor=colors.darkblue,
                alignment=1  # Center
            )
            
            report_title = report_data.get('report_metadata', {}).get('report_name', 'BI Report')
            story.append(Paragraph(report_title, title_style))
            story.append(Spacer(1, 12))
            
            # Report metadata
            metadata = report_data.get('report_metadata', {})
            if metadata:
                story.append(Paragraph("Report Information", styles['Heading2']))
                
                meta_data = [
                    ['Report ID', metadata.get('report_id', 'N/A')],
                    ['Generation Time', metadata.get('generation_time', 'N/A')],
                    ['Data Period', metadata.get('data_period', {}).get('start_date', 'N/A')],
                    ['Total Records', str(metadata.get('total_records', 'N/A'))]
                ]
                
                meta_table = Table(meta_data, colWidths=[2*inch, 4*inch])
                meta_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 14),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                    ('GRID', (0, 0), (-1, -1), 1, colors.black)
                ]))
                
                story.append(meta_table)
                story.append(Spacer(1, 12))
            
            # KPI Summary
            kpi_summary = report_data.get('kpi_summary', {})
            if kpi_summary:
                story.append(Paragraph("KPI Summary", styles['Heading2']))
                
                kpi_data = [['KPI', 'Value', 'Status', 'Trend', 'Change %']]
                for kpi_id, kpi_result in kpi_summary.items():
                    kpi_data.append([
                        kpi_id,
                        f"{kpi_result.get('value', 0):.1f}",
                        kpi_result.get('status', 'unknown'),
                        kpi_result.get('trend', 'stable'),
                        f"{kpi_result.get('change_percentage', 0):.1f}%"
                    ])
                
                kpi_table = Table(kpi_data, colWidths=[1.5*inch, 1*inch, 1*inch, 1*inch, 1*inch])
                kpi_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.darkblue),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 12),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.lightgrey),
                    ('GRID', (0, 0), (-1, -1), 1, colors.black)
                ]))
                
                story.append(kpi_table)
                story.append(Spacer(1, 12))
            
            # Include visualizations if available and requested
            if config.include_visualizations:
                visualizations = report_data.get('visualizations', {})
                if visualizations:
                    story.append(Paragraph("Visualizations", styles['Heading2']))
                    
                    for viz_name, viz_data in visualizations.items():
                        if viz_data:  # Base64 encoded image
                            try:
                                # Decode base64 image
                                image_data = base64.b64decode(viz_data.split(',')[1] if ',' in viz_data else viz_data)
                                image_buffer = io.BytesIO(image_data)
                                
                                # Add image to PDF
                                img = Image(image_buffer, width=6*inch, height=4*inch)
                                story.append(Paragraph(f"Chart: {viz_name}", styles['Heading3']))
                                story.append(img)
                                story.append(Spacer(1, 12))
                                
                            except Exception as e:
                                logger.warning(f"Failed to include visualization {viz_name}: {e}")
            
            # Executive summary
            executive_summary = report_data.get('executive_summary', '')
            if executive_summary:
                story.append(Paragraph("Executive Summary", styles['Heading2']))
                story.append(Paragraph(executive_summary, styles['Normal']))
                story.append(Spacer(1, 12))
            
            # Build PDF
            doc.build(story)
            
            return output_path
            
        except Exception as e:
            logger.error(f"PDF export failed: {e}")
            raise
    
    async def _post_process_export(self, file_path: Path, config: ExportConfig) -> Path:
        """Apply post-processing to exported file."""
        try:
            # Apply compression if requested
            if config.compression:
                file_path = await self._compress_file(file_path)
            
            # Apply password protection if requested
            if config.password_protection and config.password:
                file_path = await self._password_protect_file(file_path, config.password)
            
            return file_path
            
        except Exception as e:
            logger.error(f"Post-processing failed: {e}")
            return file_path
    
    async def _compress_file(self, file_path: Path) -> Path:
        """Compress file using ZIP."""
        try:
            compressed_path = file_path.with_suffix(file_path.suffix + '.zip')
            
            with zipfile.ZipFile(compressed_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                zipf.write(file_path, file_path.name)
            
            # Remove original file
            file_path.unlink()
            
            return compressed_path
            
        except Exception as e:
            logger.error(f"File compression failed: {e}")
            return file_path
    
    async def _password_protect_file(self, file_path: Path, password: str) -> Path:
        """Apply password protection to file."""
        try:
            # For ZIP files, use password protection
            if file_path.suffix.lower() == '.zip':
                # Re-create with password
                temp_path = file_path.with_suffix('.temp.zip')
                
                with zipfile.ZipFile(file_path, 'r') as source:
                    with zipfile.ZipFile(temp_path, 'w', zipfile.ZIP_DEFLATED) as target:
                        for item in source.infolist():
                            data = source.read(item.filename)
                            target.writestr(item, data, pwd=password.encode())
                
                file_path.unlink()
                temp_path.rename(file_path)
            
            return file_path
            
        except Exception as e:
            logger.error(f"Password protection failed: {e}")
            return file_path
    
    def _serialize_kpi_data(self, kpi_summary: Dict[str, Any]) -> Dict[str, Any]:
        """Serialize KPI data for JSON export."""
        serialized = {}
        
        for kpi_id, kpi_result in kpi_summary.items():
            if hasattr(kpi_result, '__dict__'):
                serialized[kpi_id] = kpi_result.__dict__
            else:
                serialized[kpi_id] = kpi_result
        
        return serialized
    
    def _prepare_kpi_dataframe(self, kpi_summary: Dict[str, Any]) -> pd.DataFrame:
        """Prepare KPI data as DataFrame."""
        kpi_data = []
        
        for kpi_id, kpi_result in kpi_summary.items():
            if hasattr(kpi_result, '__dict__'):
                data = {'KPI_ID': kpi_id, **kpi_result.__dict__}
            else:
                data = {'KPI_ID': kpi_id, **kpi_result}
            
            kpi_data.append(data)
        
        return pd.DataFrame(kpi_data)
    
    def _generate_html_report(self, report_data: Dict[str, Any], config: ExportConfig) -> str:
        """Generate HTML report content."""
        html_content = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Business Intelligence Report</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .header {{ background-color: #f0f0f0; padding: 20px; border-radius: 5px; }}
                .section {{ margin: 20px 0; }}
                .kpi-table {{ border-collapse: collapse; width: 100%; }}
                .kpi-table th, .kpi-table td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                .kpi-table th {{ background-color: #f2f2f2; }}
                .status-on_target {{ color: green; font-weight: bold; }}
                .status-warning {{ color: orange; font-weight: bold; }}
                .status-critical {{ color: red; font-weight: bold; }}
                .trend-up {{ color: green; }}
                .trend-down {{ color: red; }}
                .trend-stable {{ color: blue; }}
                .visualization {{ margin: 20px 0; text-align: center; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>Business Intelligence Report</h1>
                <p>Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            </div>
        """
        
        # Report metadata
        metadata = report_data.get('report_metadata', {})
        if metadata:
            html_content += f"""
            <div class="section">
                <h2>Report Information</h2>
                <p><strong>Report ID:</strong> {metadata.get('report_id', 'N/A')}</p>
                <p><strong>Report Type:</strong> {metadata.get('report_type', 'N/A')}</p>
                <p><strong>Total Records:</strong> {metadata.get('total_records', 'N/A')}</p>
            </div>
            """
        
        # KPI Summary
        kpi_summary = report_data.get('kpi_summary', {})
        if kpi_summary:
            html_content += """
            <div class="section">
                <h2>KPI Summary</h2>
                <table class="kpi-table">
                    <thead>
                        <tr>
                            <th>KPI</th>
                            <th>Value</th>
                            <th>Status</th>
                            <th>Trend</th>
                            <th>Change %</th>
                        </tr>
                    </thead>
                    <tbody>
            """
            
            for kpi_id, kpi_result in kpi_summary.items():
                status = kpi_result.get('status', 'unknown')
                trend = kpi_result.get('trend', 'stable')
                
                html_content += f"""
                        <tr>
                            <td>{kpi_id}</td>
                            <td>{kpi_result.get('value', 0):.1f}</td>
                            <td class="status-{status}">{status}</td>
                            <td class="trend-{trend}">{trend}</td>
                            <td>{kpi_result.get('change_percentage', 0):.1f}%</td>
                        </tr>
                """
            
            html_content += """
                    </tbody>
                </table>
            </div>
            """
        
        # Visualizations
        if config.include_visualizations:
            visualizations = report_data.get('visualizations', {})
            if visualizations:
                html_content += '<div class="section"><h2>Visualizations</h2>'
                
                for viz_name, viz_data in visualizations.items():
                    if viz_data:
                        html_content += f"""
                        <div class="visualization">
                            <h3>{viz_name}</h3>
                            <img src="{viz_data}" alt="{viz_name}" style="max-width: 100%; height: auto;">
                        </div>
                        """
                
                html_content += '</div>'
        
        # Executive summary
        executive_summary = report_data.get('executive_summary', '')
        if executive_summary:
            html_content += f"""
            <div class="section">
                <h2>Executive Summary</h2>
                <p>{executive_summary}</p>
            </div>
            """
        
        html_content += """
        </body>
        </html>
        """
        
        return html_content

class NotificationSystem:
    """Advanced notification system for BI reports and alerts."""
    
    def __init__(self, email_config: Optional[EmailConfig] = None):
        self.email_config = email_config
        self.notification_handlers = {
            'email': self._send_email_notification,
            'webhook': self._send_webhook_notification,
            'slack': self._send_slack_notification,
            'teams': self._send_teams_notification
        }
    
    async def send_notification(self, notification_config: NotificationConfig,
                              attachments: Optional[List[str]] = None) -> Dict[str, Any]:
        """Send notification using specified method."""
        try:
            # Validate notification type
            if notification_config.notification_type not in self.notification_handlers:
                raise ValueError(f"Unsupported notification type: {notification_config.notification_type}")
            
            # Get handler
            handler = self.notification_handlers[notification_config.notification_type]
            
            # Send notification with retries
            result = await self._send_with_retries(handler, notification_config, attachments)
            
            # Update metrics
            notifications_sent.labels(type=notification_config.notification_type).inc()
            
            logger.info(f"Notification sent successfully: {notification_config.notification_id}")
            return result
            
        except Exception as e:
            logger.error(f"Notification failed: {e}")
            return {
                'notification_id': notification_config.notification_id,
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    async def _send_with_retries(self, handler: Callable, 
                               notification_config: NotificationConfig,
                               attachments: Optional[List[str]]) -> Dict[str, Any]:
        """Send notification with retry logic."""
        for attempt in range(notification_config.retry_attempts):
            try:
                result = await handler(notification_config, attachments)
                return result
                
            except Exception as e:
                if attempt == notification_config.retry_attempts - 1:
                    raise e
                
                logger.warning(f"Notification attempt {attempt + 1} failed: {e}. Retrying...")
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
    
    async def _send_email_notification(self, notification_config: NotificationConfig,
                                     attachments: Optional[List[str]] = None) -> Dict[str, Any]:
        """Send email notification."""
        if not self.email_config:
            raise ValueError("Email configuration not provided")
        
        try:
            # Create message
            msg = MIMEMultipart()
            msg['From'] = f"{self.email_config.sender_name} <{self.email_config.sender_email or self.email_config.username}>"
            msg['To'] = ', '.join(notification_config.recipients)
            msg['Subject'] = notification_config.subject
            
            # Add body
            msg.attach(MIMEText(notification_config.message, 'html'))
            
            # Add attachments
            if attachments:
                for attachment_path in attachments:
                    if Path(attachment_path).exists():
                        with open(attachment_path, 'rb') as f:
                            attachment = MIMEApplication(f.read())
                            attachment.add_header(
                                'Content-Disposition',
                                'attachment',
                                filename=Path(attachment_path).name
                            )
                            msg.attach(attachment)
            
            # Send email
            if self.email_config.use_ssl:
                server = smtplib.SMTP_SSL(self.email_config.smtp_server, self.email_config.smtp_port)
            else:
                server = smtplib.SMTP(self.email_config.smtp_server, self.email_config.smtp_port)
                if self.email_config.use_tls:
                    server.starttls()
            
            server.login(self.email_config.username, self.email_config.password)
            server.sendmail(
                self.email_config.sender_email or self.email_config.username,
                notification_config.recipients,
                msg.as_string()
            )
            server.quit()
            
            return {
                'notification_id': notification_config.notification_id,
                'success': True,
                'timestamp': datetime.now().isoformat(),
                'recipients_count': len(notification_config.recipients)
            }
            
        except Exception as e:
            logger.error(f"Email notification failed: {e}")
            raise
    
    async def _send_webhook_notification(self, notification_config: NotificationConfig,
                                       attachments: Optional[List[str]] = None) -> Dict[str, Any]:
        """Send webhook notification."""
        try:
            import aiohttp
            
            webhook_url = notification_config.metadata.get('webhook_url')
            if not webhook_url:
                raise ValueError("Webhook URL not provided in metadata")
            
            payload = {
                'notification_id': notification_config.notification_id,
                'subject': notification_config.subject,
                'message': notification_config.message,
                'priority': notification_config.priority,
                'timestamp': datetime.now().isoformat(),
                'metadata': notification_config.metadata
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(webhook_url, json=payload) as response:
                    if response.status >= 400:
                        raise Exception(f"Webhook returned status {response.status}")
            
            return {
                'notification_id': notification_config.notification_id,
                'success': True,
                'timestamp': datetime.now().isoformat(),
                'webhook_url': webhook_url
            }
            
        except Exception as e:
            logger.error(f"Webhook notification failed: {e}")
            raise
    
    async def _send_slack_notification(self, notification_config: NotificationConfig,
                                     attachments: Optional[List[str]] = None) -> Dict[str, Any]:
        """Send Slack notification."""
        try:
            import aiohttp
            
            webhook_url = notification_config.metadata.get('slack_webhook_url')
            if not webhook_url:
                raise ValueError("Slack webhook URL not provided in metadata")
            
            # Format message for Slack
            slack_payload = {
                'text': notification_config.subject,
                'attachments': [
                    {
                        'color': self._get_slack_color(notification_config.priority),
                        'fields': [
                            {
                                'title': 'Message',
                                'value': notification_config.message,
                                'short': False
                            },
                            {
                                'title': 'Priority',
                                'value': notification_config.priority.upper(),
                                'short': True
                            },
                            {
                                'title': 'Timestamp',
                                'value': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                'short': True
                            }
                        ]
                    }
                ]
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(webhook_url, json=slack_payload) as response:
                    if response.status >= 400:
                        raise Exception(f"Slack webhook returned status {response.status}")
            
            return {
                'notification_id': notification_config.notification_id,
                'success': True,
                'timestamp': datetime.now().isoformat(),
                'platform': 'slack'
            }
            
        except Exception as e:
            logger.error(f"Slack notification failed: {e}")
            raise
    
    async def _send_teams_notification(self, notification_config: NotificationConfig,
                                     attachments: Optional[List[str]] = None) -> Dict[str, Any]:
        """Send Microsoft Teams notification."""
        try:
            import aiohttp
            
            webhook_url = notification_config.metadata.get('teams_webhook_url')
            if not webhook_url:
                raise ValueError("Teams webhook URL not provided in metadata")
            
            # Format message for Teams
            teams_payload = {
                '@type': 'MessageCard',
                '@context': 'http://schema.org/extensions',
                'themeColor': self._get_teams_color(notification_config.priority),
                'summary': notification_config.subject,
                'sections': [
                    {
                        'activityTitle': notification_config.subject,
                        'activitySubtitle': f"Priority: {notification_config.priority.upper()}",
                        'text': notification_config.message,
                        'facts': [
                            {
                                'name': 'Timestamp',
                                'value': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            },
                            {
                                'name': 'Notification ID',
                                'value': notification_config.notification_id
                            }
                        ]
                    }
                ]
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(webhook_url, json=teams_payload) as response:
                    if response.status >= 400:
                        raise Exception(f"Teams webhook returned status {response.status}")
            
            return {
                'notification_id': notification_config.notification_id,
                'success': True,
                'timestamp': datetime.now().isoformat(),
                'platform': 'teams'
            }
            
        except Exception as e:
            logger.error(f"Teams notification failed: {e}")
            raise
    
    def _get_slack_color(self, priority: str) -> str:
        """Get Slack color code for priority."""
        color_map = {
            'low': '#36a64f',      # Green
            'normal': '#439fe0',    # Blue
            'high': '#ff9900',      # Orange
            'urgent': '#ff0000'     # Red
        }
        return color_map.get(priority, '#439fe0')
    
    def _get_teams_color(self, priority: str) -> str:
        """Get Teams color code for priority."""
        color_map = {
            'low': '0078D4',      # Blue
            'normal': '0078D4',    # Blue
            'high': 'FF8C00',      # Orange
            'urgent': 'FF4B4B'     # Red
        }
        return color_map.get(priority, '0078D4')

# Integration class for complete BI export and notification
class BIExportNotificationSystem:
    """Integrated system for BI export and notifications."""
    
    def __init__(self, export_base_path: str = "./exports",
                 email_config: Optional[EmailConfig] = None):
        self.exporter = ReportExporter(export_base_path)
        self.notifier = NotificationSystem(email_config)
    
    async def export_and_notify(self, report_data: Dict[str, Any],
                              export_config: ExportConfig,
                              notification_config: Optional[NotificationConfig] = None) -> Dict[str, Any]:
        """Export report and send notification."""
        try:
            # Export report
            export_result = await self.exporter.export_report(report_data, export_config)
            
            # Send notification if configured
            notification_result = None
            if notification_config and export_result.get('success'):
                # Include export file as attachment
                attachments = [export_result['file_path']] if export_result.get('file_path') else None
                notification_result = await self.notifier.send_notification(
                    notification_config, attachments
                )
            
            return {
                'export_result': export_result,
                'notification_result': notification_result,
                'operation_timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Export and notification failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'operation_timestamp': datetime.now().isoformat()
            }
