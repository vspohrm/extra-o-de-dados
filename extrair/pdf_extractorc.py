#!/usr/bin/env python3
"""
Extrator de PDF para Markdown - Versão Adaptada
Foco na extração completa de conteúdo e salvamento em markdown
"""

import json
import sys
import re
from pathlib import Path
import datetime

# Verificar PyMuPDF
try:
    import fitz
    print("✅ PyMuPDF disponível")
except ImportError:
    print("❌ Execute: pip install PyMuPDF")
    sys.exit(1)


class PDFToMarkdownExtractor:
    """Extrator completo de PDF para Markdown com análise de estrutura."""

    @staticmethod
    def extract_page_content(doc, page_num: int) -> dict:
        """Extração completa do conteúdo de uma página."""
        page = doc[page_num]
        print(f"\n🔍 Extraindo página {page_num + 1}")
        print(f"📐 Dimensões: {page.rect.width:.0f} x {page.rect.height:.0f}")

        content = {
            "page_number": page_num + 1,
            "dimensions": {
                "width": round(page.rect.width, 2),
                "height": round(page.rect.height, 2)
            },
            "text": {
                "raw_text": "",
                "formatted_blocks": [],
                "word_count": 0,
                "has_content": False
            },
            "images": {
                "count": 0,
                "details": [],
                "extracted_paths": []
            },
            "structure": {
                "headings": [],
                "paragraphs": [],
                "lists": [],
                "tables": []
            },
            "metadata": {
                "has_charts": False,
                "has_tables": False,
                "content_type": "text"
            }
        }

        # 1. EXTRAÇÃO DE TEXTO BRUTO
        try:
            raw_text = page.get_text()
            content["text"]["raw_text"] = raw_text.strip()
            content["text"]["word_count"] = len(raw_text.split()) if raw_text else 0
            content["text"]["has_content"] = bool(raw_text.strip())
            print(f"📝 Texto extraído: {content['text']['word_count']} palavras")
        except Exception as e:
            print(f"❌ Erro na extração de texto: {e}")
            content["text"]["raw_text"] = f"[ERRO NA EXTRAÇÃO: {e}]"

        # 2. EXTRAÇÃO DE TEXTO ESTRUTURADO
        try:
            text_dict = page.get_text("dict")
            blocks = text_dict.get("blocks", [])
            
            for block in blocks:
                if "lines" in block:  # Bloco de texto
                    block_text = ""
                    for line in block["lines"]:
                        for span in line.get("spans", []):
                            text = span.get("text", "").strip()
                            if text:
                                block_text += text + " "
                    
                    if block_text.strip():
                        # Classificar tipo de bloco
                        block_info = {
                            "text": block_text.strip(),
                            "bbox": block.get("bbox", []),
                            "type": "paragraph"
                        }
                        
                        # Detectar cabeçalhos (texto em maiúsculo, fonte maior, etc.)
                        if block_text.isupper() or len(block_text) < 100:
                            if any(word in block_text.upper() for word in 
                                  ["CONFIDENTIAL", "MEMORANDUM", "FUND", "NOTICE", "REGULATORY"]):
                                block_info["type"] = "heading"
                                content["structure"]["headings"].append(block_text.strip())
                        
                        content["text"]["formatted_blocks"].append(block_info)
                        
                        if block_info["type"] == "paragraph":
                            content["structure"]["paragraphs"].append(block_text.strip())

        except Exception as e:
            print(f"❌ Erro na extração estruturada: {e}")

        # 3. EXTRAÇÃO DE IMAGENS
        try:
            images = page.get_images(full=True)
            content["images"]["count"] = len(images)
            print(f"📸 Imagens encontradas: {len(images)}")

            output_dir = Path("C:/extrair/extracted_images")
            output_dir.mkdir(parents=True, exist_ok=True)

            for i, img in enumerate(images):
                try:
                    pix = fitz.Pixmap(doc, img[0])
                    
                    # Tratar colorspace problemático
                    if pix.colorspace and pix.colorspace.name in ["DeviceN", "Separation", "Lab", "ICCBased"]:
                        pix_rgb = fitz.Pixmap(fitz.csRGB, pix)
                        pix = pix_rgb

                    pixels = pix.width * pix.height
                    aspect = round(pix.width / pix.height, 2) if pix.height > 0 else 0
                    
                    img_detail = {
                        "index": i + 1,
                        "dimensions": f"{pix.width}x{pix.height}",
                        "pixels": pixels,
                        "aspect_ratio": aspect,
                        "size_mb": round(len(pix.tobytes()) / 1024 / 1024, 3),
                        "filename": f"page_{page_num + 1}_image_{i + 1}.png"
                    }
                    
                    # Salvar imagem
                    img_file = output_dir / img_detail["filename"]
                    pix.save(str(img_file))
                    content["images"]["extracted_paths"].append(str(img_file))
                    
                    # Classificar se pode ser gráfico
                    if pixels > 50000 and 0.5 <= aspect <= 3.0:
                        img_detail["likely_chart"] = True
                        content["metadata"]["has_charts"] = True
                    else:
                        img_detail["likely_chart"] = False
                    
                    content["images"]["details"].append(img_detail)
                    print(f" 💾 Imagem salva: {img_file}")
                    pix = None
                    
                except Exception as e:
                    print(f"❌ Erro ao processar imagem {i}: {e}")
                    content["images"]["details"].append({
                        "index": i + 1, 
                        "error": str(e)
                    })

        except Exception as e:
            print(f"❌ Erro na extração de imagens: {e}")

        # 4. DETECTAR TABELAS (baseado em padrões de texto)
        try:
            text_lines = content["text"]["raw_text"].split('\n')
            potential_tables = []
            
            for i, line in enumerate(text_lines):
                # Detectar linhas com múltiplos números separados por espaços/tabs
                if re.search(r'\d+\s+\d+\s+\d+', line) or '\t' in line:
                    potential_tables.append({
                        "line_number": i + 1,
                        "content": line.strip()
                    })
            
            if potential_tables:
                content["structure"]["tables"] = potential_tables
                content["metadata"]["has_tables"] = True
                print(f"📊 Possíveis tabelas detectadas: {len(potential_tables)}")

        except Exception as e:
            print(f"❌ Erro na detecção de tabelas: {e}")

        # 5. CLASSIFICAR TIPO DE CONTEÚDO
        if content["metadata"]["has_charts"]:
            content["metadata"]["content_type"] = "chart"
        elif content["metadata"]["has_tables"]:
            content["metadata"]["content_type"] = "table"
        elif len(content["structure"]["headings"]) > 0:
            content["metadata"]["content_type"] = "structured_document"
        else:
            content["metadata"]["content_type"] = "text"

        print(f"📋 Tipo de conteúdo: {content['metadata']['content_type']}")
        return content

    @staticmethod
    def generate_markdown(extracted_data: dict, doc_name: str) -> str:
        """Gerar markdown estruturado a partir dos dados extraídos."""
        
        md_content = []
        
        # Cabeçalho do documento
        md_content.append(f"# {doc_name}")
        md_content.append(f"**Extraído em:** {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        md_content.append(f"**Total de páginas:** {len(extracted_data['pages'])}")
        md_content.append("")
        
        # Resumo executivo
        md_content.append("## Resumo Executivo")
        md_content.append("")
        
        total_words = sum(page["text"]["word_count"] for page in extracted_data["pages"].values())
        total_images = sum(page["images"]["count"] for page in extracted_data["pages"].values())
        chart_pages = [p for p in extracted_data["pages"].values() if p["metadata"]["has_charts"]]
        table_pages = [p for p in extracted_data["pages"].values() if p["metadata"]["has_tables"]]
        
        md_content.append(f"- **Palavras totais:** {total_words:,}")
        md_content.append(f"- **Imagens totais:** {total_images}")
        md_content.append(f"- **Páginas com gráficos:** {len(chart_pages)}")
        md_content.append(f"- **Páginas com tabelas:** {len(table_pages)}")
        md_content.append("")
        
        # Conteúdo página por página
        md_content.append("## Conteúdo por Página")
        md_content.append("")
        
        for page_key in sorted(extracted_data["pages"].keys(), key=lambda x: int(x.split('_')[1])):
            page_data = extracted_data["pages"][page_key]
            page_num = page_data["page_number"]
            
            md_content.append(f"### Página {page_num}")
            md_content.append("")
            
            # Metadados da página
            md_content.append(f"**Tipo:** {page_data['metadata']['content_type']}")
            md_content.append(f"**Palavras:** {page_data['text']['word_count']}")
            md_content.append(f"**Imagens:** {page_data['images']['count']}")
            md_content.append("")
            
            # Cabeçalhos detectados
            if page_data["structure"]["headings"]:
                md_content.append("#### Cabeçalhos Detectados")
                for heading in page_data["structure"]["headings"]:
                    md_content.append(f"- {heading}")
                md_content.append("")
            
            # Imagens
            if page_data["images"]["count"] > 0:
                md_content.append("#### Imagens")
                for img in page_data["images"]["details"]:
                    if "error" not in img:
                        chart_indicator = " 📊 (Possível gráfico)" if img.get("likely_chart", False) else ""
                        md_content.append(f"- **Imagem {img['index']}:** {img['dimensions']} ({img['pixels']:,} pixels){chart_indicator}")
                        if "filename" in img:
                            md_content.append(f"  - Arquivo: `{img['filename']}`")
                md_content.append("")
            
            # Tabelas detectadas
            if page_data["structure"]["tables"]:
                md_content.append("#### Tabelas Detectadas")
                for table in page_data["structure"]["tables"][:5]:  # Mostrar apenas 5 primeiras
                    md_content.append(f"```")
                    md_content.append(table["content"])
                    md_content.append(f"```")
                if len(page_data["structure"]["tables"]) > 5:
                    md_content.append(f"*(... e mais {len(page_data['structure']['tables']) - 5} linhas)*")
                md_content.append("")
            
            # Texto da página (limitado para não ficar muito longo)
            if page_data["text"]["has_content"]:
                md_content.append("#### Conteúdo Textual")
                text_content = page_data["text"]["raw_text"]
                
                # Limitar tamanho do texto exibido
                if len(text_content) > 3000:
                    text_content = text_content[:3000] + "\n\n*(... conteúdo truncado)*"
                
                # Preservar quebras de linha importantes
                text_content = re.sub(r'\n\s*\n', '\n\n', text_content)
                md_content.append("```")
                md_content.append(text_content)
                md_content.append("```")
                md_content.append("")
            
            md_content.append("---")
            md_content.append("")
        
        # Apêndices
        md_content.append("## Apêndices")
        md_content.append("")
        
        # Lista de todas as imagens extraídas
        all_images = []
        for page_data in extracted_data["pages"].values():
            all_images.extend(page_data["images"]["extracted_paths"])
        
        if all_images:
            md_content.append("### Imagens Extraídas")
            for img_path in all_images:
                md_content.append(f"- `{img_path}`")
            md_content.append("")
        
        return '\n'.join(md_content)


def extract_pdf_to_markdown(file_path: str, output_dir: str = "C:/extrair"):
    """Extração completa de PDF para markdown."""
    
    file_path = Path(file_path)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"🚀 EXTRAÇÃO PDF PARA MARKDOWN")
    print(f"📄 Arquivo: {file_path.name}")
    print(f"📁 Saída: {output_dir}")
    
    try:
        doc = fitz.open(str(file_path))
        print(f"📊 Total de páginas: {len(doc)}")
        
        # Dados extraídos
        extracted_data = {
            "document": file_path.name,
            "source_path": str(file_path),
            "total_pages": len(doc),
            "extraction_timestamp": str(datetime.datetime.now()),
            "pages": {}
        }
        
        # Extrair cada página
        for page_num in range(len(doc)):
            page_content = PDFToMarkdownExtractor.extract_page_content(doc, page_num)
            page_key = f"page_{page_num + 1}"
            extracted_data["pages"][page_key] = page_content
        
        # Gerar markdown
        print(f"\n📝 Gerando markdown...")
        markdown_content = PDFToMarkdownExtractor.generate_markdown(extracted_data, file_path.stem)
        
        # Salvar arquivos
        # 1. Markdown principal
        md_file = output_dir / f"{file_path.stem}_extracted.md"
        with open(md_file, 'w', encoding='utf-8') as f:
            f.write(markdown_content)
        print(f"💾 Markdown salvo: {md_file}")
        
        # 2. JSON com dados estruturados
        json_file = output_dir / f"{file_path.stem}_data.json"
        clean_data = json.loads(json.dumps(extracted_data, default=str))
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(clean_data, f, indent=2, ensure_ascii=False)
        print(f"💾 Dados JSON salvos: {json_file}")
        
        # 3. Arquivo de texto bruto (todas as páginas)
        txt_file = output_dir / f"{file_path.stem}_raw_text.txt"
        with open(txt_file, 'w', encoding='utf-8') as f:
            f.write(f"EXTRAÇÃO DE TEXTO BRUTO - {file_path.name}\n")
            f.write(f"Data: {datetime.datetime.now()}\n")
            f.write("="*80 + "\n\n")
            
            for page_key in sorted(extracted_data["pages"].keys(), key=lambda x: int(x.split('_')[1])):
                page_data = extracted_data["pages"][page_key]
                f.write(f"\n--- PÁGINA {page_data['page_number']} ---\n\n")
                f.write(page_data["text"]["raw_text"])
                f.write("\n\n")
        print(f"💾 Texto bruto salvo: {txt_file}")
        
        # Resumo final
        total_words = sum(page["text"]["word_count"] for page in extracted_data["pages"].values())
        total_images = sum(page["images"]["count"] for page in extracted_data["pages"].values())
        
        print(f"\n✅ EXTRAÇÃO CONCLUÍDA!")
        print(f"📊 Estatísticas:")
        print(f" - Total de palavras extraídas: {total_words:,}")
        print(f" - Total de imagens extraídas: {total_images}")
        print(f" - Arquivos gerados: 3 (markdown, JSON, texto bruto)")
        print(f" - Diretório de saída: {output_dir}")
        
        doc.close()
        return extracted_data
        
    except Exception as e:
        print(f"❌ Erro na extração: {e}")
        return None


def main():
    """Função principal."""
    print("🚀 Extrator PDF para Markdown")
    
    if len(sys.argv) < 2:
        print("\n💡 COMO USAR:")
        print(" python pdf_extractor.py <arquivo.pdf>")
        print(" python pdf_extractor.py <arquivo.pdf> <diretorio_saida>")
        print("\nExemplos:")
        print(" python pdf_extractor.py documento.pdf")
        print(" python pdf_extractor.py documento.pdf C:/minha_pasta")
        return

    pdf_file = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else "C:/extrair"
    
    if not Path(pdf_file).exists():
        print(f"❌ Arquivo não encontrado: {pdf_file}")
        return
    
    # Executar extração
    extract_pdf_to_markdown(pdf_file, output_dir)


if __name__ == "__main__":
    print("\n💡 EXEMPLO PARA SEU ARQUIVO:")
    print(r" python pdf_extractor.py 'C:\extrair\DataService-Controller-PdfServerAqUAA6 (1).pdf'")
    print()
    main()