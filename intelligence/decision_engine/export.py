from pathlib import Path
from loguru import logger
import markdownify
import json

def cam_to_docx(job_dir: Path) -> Path:
    cam_md_path = job_dir / "decision_engine" / "cam.md"
    out_path = job_dir / "decision_engine" / "cam.docx"
    
    if not cam_md_path.exists():
        return None
        
    try:
        from docx import Document
        doc = Document()
        doc.add_heading('Credit Approval Memo (CAM)', 0)
        
        with open(cam_md_path, "r", encoding="utf-8") as f:
            content = f.read()
            
        for line in content.split('
'):
            line = line.strip()
            if not line:
                continue
            if line.startswith('# '):
                doc.add_heading(line[2:], level=1)
            elif line.startswith('## '):
                doc.add_heading(line[3:], level=2)
            elif line.startswith('- '):
                doc.add_paragraph(line[2:], style='List Bullet')
            else:
                doc.add_paragraph(line)
                
        doc.save(str(out_path))
        return out_path
    except Exception as e:
        logger.error(f"Failed to create DOCX: {e}")
        return None

def cam_to_pdf(job_dir: Path) -> Path:
    cam_md_path = job_dir / "decision_engine" / "cam.md"
    out_path = job_dir / "decision_engine" / "cam.pdf"
    
    if not cam_md_path.exists():
        return None
        
    try:
        from reportlab.lib.pagesizes import letter
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
        from reportlab.lib.styles import getSampleStyleSheet
        
        doc = SimpleDocTemplate(str(out_path), pagesize=letter)
        styles = getSampleStyleSheet()
        Story = []
        
        with open(cam_md_path, "r", encoding="utf-8") as f:
            content = f.read()
            
        for line in content.split('
'):
            line = line.strip()
            if not line:
                Story.append(Spacer(1, 12))
                continue
            
            # Simple conversion
            line = line.replace("**", "<b>").replace("**", "</b>") # Needs real regex for toggle, this is a hack, Reportlab handles bold with <b>
            # basic fix for bold
            import re
            line = re.sub(r'\*\*(.*?)\*\*', r'<b>\1</b>', line)
            
            if line.startswith('# '):
                Story.append(Paragraph(line[2:], styles['Heading1']))
            elif line.startswith('## '):
                Story.append(Paragraph(line[3:], styles['Heading2']))
            elif line.startswith('- '):
                Story.append(Paragraph(line, styles['Bullet']))
            else:
                Story.append(Paragraph(line, styles['Normal']))
                
        doc.build(Story)
        return out_path
    except Exception as e:
        logger.error(f"Failed to create PDF: {e}")
        return None
