#!/usr/bin/env python3
"""
Extrator de PDF para Chunks de Contexto - Versão para LLM
Foco na extração por chunks semânticos mantendo contexto
"""

import json
import sys
import re
from pathlib import Path
import datetime
from typing import List, Dict, Any

# Verificar PyMuPDF
try:
    import fitz
    print("✅ PyMuPDF disponível")
except ImportError:
    print("❌ Execute: pip install PyMuPDF")
    sys.exit(1)


class PDFToChunksExtractor:
    """Extrator de PDF em chunks contextuais para LLM."""

    def __init__(self, chunk_size: int = 1000, overlap: int = 200):
        self.chunk_size = chunk_size  # Tamanho ideal do chunk em caracteres
        self.overlap = overlap        # Sobreposição entre chunks
        self.min_chunk_size = 100     # Tamanho mínimo para formar um chunk
        
    @staticmethod
    def extract_document_metadata(doc) -> Dict[str, Any]:
        """Extrai metadados do documento."""
        try:
            metadata = doc.metadata
            return {
                "title": metadata.get("title", ""),
                "author": metadata.get("author", ""),
                "subject": metadata.get("subject", ""),
                "creator": metadata.get("creator", ""),
                "producer": metadata.get("producer", ""),
                "creation_date": metadata.get("creationDate", ""),
                "modification_date": metadata.get("modDate", ""),
                "total_pages": len(doc),
                "extraction_timestamp": str(datetime.datetime.now())
            }
        except Exception as e:
            return {
                "error": str(e),
                "total_pages": len(doc),
                "extraction_timestamp": str(datetime.datetime.now())
            }

    def extract_page_elements(self, doc, page_num: int) -> Dict[str, Any]:
        """Extrai todos os elementos de uma página."""
        page = doc[page_num]
        print(f"\n🔍 Analisando página {page_num + 1}")
        
        elements = {
            "page_number": page_num + 1,
            "dimensions": {
                "width": round(page.rect.width, 2),
                "height": round(page.rect.height, 2)
            },
            "text_content": "",
            "structured_blocks": [],
            "images": [],
            "tables": [],
            "visual_elements": {
                "has_charts": False,
                "has_diagrams": False,
                "has_tables": False
            }
        }
        
        # 1. EXTRAIR TEXTO BRUTO
        try:
            raw_text = page.get_text()
            elements["text_content"] = raw_text.strip()
            print(f"📝 Texto extraído: {len(raw_text.split())} palavras")
        except Exception as e:
            print(f"❌ Erro na extração de texto: {e}")
            elements["text_content"] = f"[ERRO NA EXTRAÇÃO: {e}]"

        # 2. EXTRAIR BLOCOS ESTRUTURADOS
        try:
            text_dict = page.get_text("dict")
            blocks = text_dict.get("blocks", [])
            
            for i, block in enumerate(blocks):
                if "lines" in block:  # Bloco de texto
                    block_text = ""
                    font_info = []
                    
                    for line in block["lines"]:
                        for span in line.get("spans", []):
                            text = span.get("text", "").strip()
                            if text:
                                block_text += text + " "
                                # Capturar informações de formatação
                                font_info.append({
                                    "font": span.get("font", ""),
                                    "size": round(span.get("size", 0), 1),
                                    "flags": span.get("flags", 0),
                                    "color": span.get("color", 0)
                                })
                    
                    if block_text.strip():
                        # Classificar tipo de bloco baseado em conteúdo e formatação
                        block_type = self._classify_block_type(block_text, font_info)
                        
                        structured_block = {
                            "id": f"page_{page_num + 1}_block_{i + 1}",
                            "type": block_type,
                            "content": block_text.strip(),
                            "bbox": block.get("bbox", []),
                            "font_info": font_info[0] if font_info else {},
                            "position": {
                                "top": round(block.get("bbox", [0,0,0,0])[1], 1),
                                "left": round(block.get("bbox", [0,0,0,0])[0], 1)
                            }
                        }
                        
                        elements["structured_blocks"].append(structured_block)

        except Exception as e:
            print(f"❌ Erro na extração estruturada: {e}")

        # 3. DETECTAR IMAGENS E GRÁFICOS
        try:
            images = page.get_images(full=True)
            elements["images"] = []
            
            for i, img in enumerate(images):
                try:
                    # Usar método extract_image para informações básicas
                    img_data = doc.extract_image(img[0])
                    
                    image_info = {
                        "id": f"page_{page_num + 1}_image_{i + 1}",
                        "dimensions": f"{img_data['width']}x{img_data['height']}",
                        "size_bytes": len(img_data["image"]),
                        "format": img_data["ext"],
                        "colorspace": img_data["colorspace"],
                        "position_ref": img[0],  # Referência para localização
                        "likely_type": self._classify_image_type(img_data)
                    }
                    
                    elements["images"].append(image_info)
                    
                    # Atualizar flags visuais
                    if image_info["likely_type"] in ["chart", "graph"]:
                        elements["visual_elements"]["has_charts"] = True
                    elif image_info["likely_type"] in ["diagram", "flowchart"]:
                        elements["visual_elements"]["has_diagrams"] = True
                        
                except Exception as img_error:
                    print(f"⚠️ Erro ao processar imagem {i + 1}: {img_error}")
                    elements["images"].append({
                        "id": f"page_{page_num + 1}_image_{i + 1}",
                        "error": str(img_error)
                    })
            
            print(f"📸 Imagens processadas: {len(elements['images'])}")

        except Exception as e:
            print(f"❌ Erro na detecção de imagens: {e}")

        # 4. DETECTAR TABELAS
        try:
            tables = self._detect_tables(elements["text_content"])
            elements["tables"] = tables
            if tables:
                elements["visual_elements"]["has_tables"] = True
                print(f"📊 Tabelas detectadas: {len(tables)}")
        except Exception as e:
            print(f"❌ Erro na detecção de tabelas: {e}")

        return elements

    def _classify_block_type(self, text: str, font_info: List[Dict]) -> str:
        """Classifica o tipo de bloco baseado em características estruturais e de conteúdo."""
        
        if not text or not text.strip():
            return "empty"
        
        text_clean = text.strip()
        text_upper = text_clean.upper()
        
        # 1. ANÁLISE DE FORMATAÇÃO (se disponível)
        is_bold = False
        is_large_font = False
        if font_info:
            font = font_info[0] if isinstance(font_info, list) and font_info else font_info
            # Flags bit 4 = bold, bit 1 = italic
            is_bold = bool(font.get("flags", 0) & 16)
            font_size = font.get("size", 10)
            is_large_font = font_size > 12
        
        # 2. CARACTERÍSTICAS ESTRUTURAIS
        
        # É muito curto (provavelmente título, número de página, etc.)
        if len(text_clean) < 10:
            return "label"
        
        # Começa com numeração de página ou similar
        if re.match(r'^\s*\d+\s*$', text_clean):  # Só números
            return "page_number"
        
        # 3. DETECÇÃO DE CABEÇALHOS/TÍTULOS
        
        # Por formatação (negrito + fonte grande)
        if is_bold and is_large_font:
            return "heading"
        
        # Por posição estrutural (texto curto + maiúsculo)
        if len(text_clean) < 100 and text_clean.isupper():
            return "heading"
        
        # Por padrões de título (independente de palavras específicas)
        heading_patterns = [
            r'^[A-Z][A-Z\s&/\-:]{10,}$',  # TEXTO EM MAIÚSCULO
            r'^\d+\.\s*[A-Z]',             # 1. Título Numerado
            r'^Chapter\s+\d+',             # Chapter 1, Capítulo 1, etc.
            r'^Section\s+[A-Z\d]',         # Section A, Seção 1, etc.
            r'^[A-Z][a-z]+\s+(Statement|Report|Overview|Summary|Analysis)', # Investment Report, etc.
        ]
        
        if any(re.match(pattern, text_clean, re.IGNORECASE) for pattern in heading_patterns):
            return "heading"
        
        # Texto curto com palavras-chave comuns em títulos financeiros
        if len(text_clean) < 150:
            title_indicators = [
                'statement', 'report', 'overview', 'summary', 'analysis',
                'performance', 'portfolio', 'investment', 'fund', 'holdings',
                'financial', 'results', 'income', 'balance', 'cash flow',
                'risk', 'disclosure', 'governance', 'audit', 'compliance'
            ]
            if any(word in text_upper for word in [w.upper() for w in title_indicators]):
                return "heading"
        
        # 4. DETECÇÃO DE LISTAS
        
        # Lista com bullets ou numeração
        list_patterns = [
            r'^\s*[-•▪▫◦‣⁃]\s+',           # Bullets variados
            r'^\s*\d+\.\s+',               # 1. 2. 3.
            r'^\s*\([a-zA-Z0-9]+\)\s+',    # (a) (b) (1) (2)
            r'^\s*[a-zA-Z]\.\s+',          # a. b. c.
            r'^\s*[ivxlc]+\.\s+',          # i. ii. iii. (romano)
        ]
        
        if any(re.match(pattern, text_clean) for pattern in list_patterns):
            return "list_item"
        
        # 5. DETECÇÃO DE DADOS TABULARES
        
        # Múltiplos números com separação tabular
        numbers = re.findall(r'\b\d+(?:,\d{3})*(?:\.\d+)?%?\b', text_clean)
        has_tabs = '\t' in text_clean
        has_multiple_spaces = '  ' in text_clean or '   ' in text_clean
        
        if len(numbers) > 2 and (has_tabs or has_multiple_spaces):
            return "table_data"
        
        # Padrões típicos de tabelas financeiras
        table_patterns = [
            r'\$\s*[\d,]+(?:\.\d{2})?',     # Valores monetários $1,000.00
            r'\d+\.\d+\s*%',                # Percentuais 15.2%
            r'\(\d+\)',                     # Números negativos (1000)
            r'\d{4}\s+\d+\.\d+\s*%',        # Ano + percentual
        ]
        
        table_matches = sum(1 for pattern in table_patterns if re.search(pattern, text_clean))
        if table_matches >= 2:
            return "table_data"
        
        # 6. DETECÇÃO DE NOTAS E REFERÊNCIAS
        
        # Notas de rodapé típicas
        if len(text_clean) < 300 and any(text_clean.startswith(prefix) for prefix in ['*', '†', '‡', 'Note:', 'See:', 'Source:']):
            return "footnote"
        
        # Referências e disclaimers
        disclaimer_patterns = [
            r'past performance',
            r'not a guarantee',
            r'risk of loss',
            r'see disclaimer',
            r'important information',
            r'for more information'
        ]
        
        if len(text_clean) < 500 and any(re.search(pattern, text_upper) for pattern in [p.upper() for p in disclaimer_patterns]):
            return "disclaimer"
        
        # 7. DETECÇÃO DE CONTEÚDO PRINCIPAL
        
        # Parágrafos longos (conteúdo principal)
        if len(text_clean) > 200:
            return "paragraph"
        
        # Texto médio que não se encaixa em outras categorias
        if len(text_clean) > 50:
            return "text_block"
        
        # 8. CASOS ESPECIAIS
        
        # Datas
        if re.match(r'^\s*\w+\s+\d{1,2},\s+\d{4}\s*$', text_clean):  # March 14, 2025
            return "date"
        
        # URLs e emails
        if re.match(r'^https?://|^www\.|@.*\.com', text_clean):
            return "contact_info"
        
        # Default
        return "text_block"





    def _classify_image_type(self, img_data: Dict) -> str:
        """Classifica o tipo de imagem baseado nas características."""
        width = img_data.get("width", 0)
        height = img_data.get("height", 0)
        pixels = width * height
        aspect_ratio = width / height if height > 0 else 1
        
        # Critérios para gráficos
        if (pixels > 50000 and 
            0.5 <= aspect_ratio <= 3.0 and
            width > 300 and height > 200):
            return "chart"
        
        # Critérios para diagramas
        if pixels > 20000 and aspect_ratio > 2.5:
            return "diagram"
        
        # Imagens pequenas podem ser ícones ou logos
        if pixels < 20000:
            return "icon"
        
        return "image"

    def _detect_tables(self, text: str) -> List[Dict]:
        """Detecta tabelas no texto."""
        tables = []
        lines = text.split('\n')
        
        current_table = []
        for i, line in enumerate(lines):
            line = line.strip()
            
            # Detectar linha de tabela (múltiplos números/dados separados)
            if (re.search(r'\d+.*\d+.*\d+', line) or 
                '\t' in line or 
                len(re.findall(r'\s{3,}', line)) >= 2):
                
                current_table.append({
                    "line_number": i + 1,
                    "content": line
                })
            else:
                # Se temos uma tabela em construção e a linha atual não é tabela
                if len(current_table) >= 2:  # Pelo menos 2 linhas para ser tabela
                    tables.append({
                        "id": f"table_{len(tables) + 1}",
                        "start_line": current_table[0]["line_number"],
                        "end_line": current_table[-1]["line_number"],
                        "rows": current_table,
                        "row_count": len(current_table)
                    })
                current_table = []
        
        # Verificar tabela no final
        if len(current_table) >= 2:
            tables.append({
                "id": f"table_{len(tables) + 1}",
                "start_line": current_table[0]["line_number"],
                "end_line": current_table[-1]["line_number"],
                "rows": current_table,
                "row_count": len(current_table)
            })
        
        return tables

    def create_content_chunks(self, all_elements: List[Dict]) -> List[Dict]:
        """Cria chunks de conteúdo mantendo contexto semântico."""
        print(f"\n📦 Criando chunks de conteúdo...")
        
        chunks = []
        current_chunk = {
            "id": "",
            "content": "",
            "metadata": {
                "pages": [],
                "elements": [],
                "visual_elements": {},
                "content_types": [],
                "word_count": 0,
                "char_count": 0
            },
            "context": {
                "previous_chunk_summary": "",
                "section_context": "",
                "document_position": ""
            }
        }
        
        chunk_counter = 1
        total_content = ""
        
        # Processar elementos por página mantendo ordem
        for page_elements in all_elements:
            page_num = page_elements["page_number"]
            print(f"📄 Processando página {page_num} para chunks...")
            
            # Adicionar contexto de imagens e tabelas da página
            visual_context = ""
            if page_elements["images"]:
                visual_context += f"\n[PÁGINA {page_num} CONTÉM {len(page_elements['images'])} IMAGENS]"
                for img in page_elements["images"]:
                    if img.get("likely_type") in ["chart", "graph"]:
                        visual_context += f"\n[GRÁFICO: {img['dimensions']}]"
            
            if page_elements["tables"]:
                visual_context += f"\n[PÁGINA {page_num} CONTÉM {len(page_elements['tables'])} TABELAS]"
            
            # Processar blocos estruturados em ordem
            for block in page_elements["structured_blocks"]:
                block_content = block["content"]
                block_type = block["type"]
                
                # Adicionar contexto visual se relevante
                content_with_context = block_content
                if visual_context and block_type in ["heading", "paragraph"]:
                    content_with_context = block_content + visual_context
                    visual_context = ""  # Usar apenas uma vez
                
                # Verificar se precisa criar novo chunk
                if (len(current_chunk["content"]) + len(content_with_context) > self.chunk_size and 
                    len(current_chunk["content"]) > self.min_chunk_size):
                    
                    # Finalizar chunk atual
                    self._finalize_chunk(current_chunk, chunk_counter, total_content)
                    chunks.append(current_chunk)
                    
                    # Criar novo chunk com sobreposição
                    overlap_content = self._get_overlap_content(current_chunk["content"])
                    previous_summary = self._create_chunk_summary(current_chunk["content"])
                    
                    chunk_counter += 1
                    current_chunk = {
                        "id": f"chunk_{chunk_counter}",
                        "content": overlap_content,
                        "metadata": {
                            "pages": [page_num],
                            "elements": [block["id"]],
                            "visual_elements": page_elements["visual_elements"],
                            "content_types": [block_type],
                            "word_count": 0,
                            "char_count": 0
                        },
                        "context": {
                            "previous_chunk_summary": previous_summary,
                            "section_context": self._get_section_context(block_content),
                            "document_position": f"~{len(chunks) * 100 / len(all_elements):.0f}% do documento"
                        }
                    }
                
                # Adicionar conteúdo ao chunk atual
                if current_chunk["content"]:
                    current_chunk["content"] += "\n\n"
                current_chunk["content"] += content_with_context
                
                # Atualizar metadados
                if page_num not in current_chunk["metadata"]["pages"]:
                    current_chunk["metadata"]["pages"].append(page_num)
                current_chunk["metadata"]["elements"].append(block["id"])
                if block_type not in current_chunk["metadata"]["content_types"]:
                    current_chunk["metadata"]["content_types"].append(block_type)
                
                total_content += content_with_context + "\n"
        
        # Finalizar último chunk
        if current_chunk["content"].strip():
            self._finalize_chunk(current_chunk, chunk_counter, total_content)
            chunks.append(current_chunk)
        
        print(f"✅ Criados {len(chunks)} chunks de conteúdo")
        
        # Adicionar links entre chunks para contexto
        for i, chunk in enumerate(chunks):
            chunk["context"]["chunk_position"] = f"{i + 1}/{len(chunks)}"
            if i > 0:
                chunk["context"]["previous_chunk_id"] = chunks[i - 1]["id"]
            if i < len(chunks) - 1:
                chunk["context"]["next_chunk_id"] = chunks[i + 1]["id"]
        
        return chunks

    def _finalize_chunk(self, chunk: Dict, chunk_id: int, total_content: str):
        """Finaliza um chunk calculando metadados."""
        chunk["id"] = f"chunk_{chunk_id}"
        chunk["metadata"]["word_count"] = len(chunk["content"].split())
        chunk["metadata"]["char_count"] = len(chunk["content"])
        
        # Calcular posição relativa no documento
        content_so_far = len(chunk["content"])
        total_length = len(total_content)
        if total_length > 0:
            chunk["context"]["document_position"] = f"~{(content_so_far / total_length) * 100:.1f}% do documento"

    def _get_overlap_content(self, content: str) -> str:
        """Obtém conteúdo de sobreposição do chunk anterior."""
        if len(content) <= self.overlap:
            return content
        
        # Tentar encontrar uma quebra natural (sentença completa)
        overlap_start = len(content) - self.overlap
        sentences = re.split(r'[.!?]\s+', content[overlap_start:])
        
        if len(sentences) > 1:
            return '. '.join(sentences[1:]) + '.'
        else:
            return content[-self.overlap:]

    def _create_chunk_summary(self, content: str) -> str:
        """Cria um resumo simples do chunk anterior."""
        words = content.split()
        if len(words) <= 20:
            return content
        
        # Pegar as primeiras e últimas palavras para contexto
        summary = ' '.join(words[:10]) + " ... " + ' '.join(words[-10:])
        return summary

    def _get_section_context(self, content: str) -> str:
        """Identifica o contexto da seção atual de forma genérica."""
        content_upper = content.upper()
        content_clean = ' '.join(content.split())  # Remove espaços extras
        
        # Contadores para análise ponderada
        context_scores = {
            "document_header": 0,
            "executive_summary": 0,
            "financial_data": 0,
            "investment_content": 0,
            "risk_section": 0,
            "legal_regulatory": 0,
            "governance": 0,
            "contact_info": 0,
            "performance_data": 0,
            "general_content": 0
        }
        
        # 1. CABEÇALHO/TÍTULO DO DOCUMENTO
        header_indicators = [
            "confidential", "memorandum", "prospectus", "offering", "circular",
            "annual report", "interim report", "quarterly report", "half year",
            "private placement", "supplement", "addendum", "amendment"
        ]
        
        for indicator in header_indicators:
            if indicator in content_upper:
                context_scores["document_header"] += 3
        
        # Padrões estruturais de cabeçalho
        if len(content_clean) < 200 and any(pattern in content_upper for pattern in [
            "LTD", "LLC", "INC", "CORP", "SA", "GMBH", "PLC", "NV"
        ]):
            context_scores["document_header"] += 2
        
        # 2. RESUMO EXECUTIVO
        summary_indicators = [
            "executive summary", "overview", "highlights", "key points",
            "summary", "introduction", "at a glance", "snapshot"
        ]
        
        for indicator in summary_indicators:
            if indicator in content_upper:
                context_scores["executive_summary"] += 2
        
        # 3. DADOS FINANCEIROS (análise mais sofisticada)
        financial_patterns = [
            r'\$[\d,]+(?:\.\d{2})?(?:\s*million|\s*billion|\s*thousand)?',  # $1.5 million
            r'[\d,]+\.\d+%',                                               # 15.25%
            r'\b\d{1,3}(?:,\d{3})+\b',                                    # 1,000,000
            r'USD|EUR|GBP|JPY|CHF|AUD|CAD',                               # Moedas
            r'\(\$?[\d,]+\)',                                             # Valores negativos
            r'NAV|AUM|Assets Under Management',                           # Termos financeiros
            r'basis points|bps',                                          # Pontos base
            r'P/E|ROE|ROA|EBITDA|WACC',                                  # Métricas financeiras
        ]
        
        financial_matches = 0
        for pattern in financial_patterns:
            financial_matches += len(re.findall(pattern, content, re.IGNORECASE))
        
        context_scores["financial_data"] += min(financial_matches * 0.5, 3)
        
        # 4. CONTEÚDO DE INVESTIMENTO
        investment_indicators = [
            "investment", "portfolio", "fund", "asset", "allocation", "strategy",
            "manager", "management", "performance", "returns", "benchmark",
            "equity", "fixed income", "alternative", "derivative", "hedge",
            "long", "short", "position", "holding", "security", "instrument"
        ]
        
        investment_count = sum(1 for indicator in investment_indicators if indicator in content_upper)
        context_scores["investment_content"] += min(investment_count * 0.3, 3)
        
        # 5. SEÇÃO DE RISCOS
        risk_indicators = [
            "risk", "warning", "caution", "disclaimer", "limitation",
            "uncertainty", "volatile", "loss", "adverse", "fluctuation",
            "market risk", "credit risk", "liquidity risk", "operational risk",
            "concentration risk", "currency risk", "interest rate risk"
        ]
        
        risk_count = sum(1 for indicator in risk_indicators if indicator in content_upper)
        context_scores["risk_section"] += min(risk_count * 0.4, 3)
        
        # Padrões típicos de disclaimers
        disclaimer_patterns = [
            "past performance", "not guarantee", "may lose", "no assurance",
            "should not rely", "consult", "advisor", "professional advice"
        ]
        
        disclaimer_count = sum(1 for pattern in disclaimer_patterns if pattern in content_upper)
        context_scores["risk_section"] += min(disclaimer_count * 0.5, 2)
        
        # 6. LEGAL/REGULATÓRIO
        legal_indicators = [
            "legal", "regulatory", "compliance", "regulation", "law", "statute",
            "sec", "cftc", "finra", "mifid", "ucits", "aifmd", "fatca",
            "tax", "taxation", "withholding", "jurisdiction", "governing law",
            "litigation", "proceeding", "audit", "examination"
        ]
        
        legal_count = sum(1 for indicator in legal_indicators if indicator in content_upper)
        context_scores["legal_regulatory"] += min(legal_count * 0.4, 3)
        
        # 7. GOVERNANÇA CORPORATIVA
        governance_indicators = [
            "board", "director", "governance", "committee", "shareholder",
            "voting", "election", "appointment", "remuneration", "compensation",
            "independence", "oversight", "fiduciary", "stewardship"
        ]
        
        governance_count = sum(1 for indicator in governance_indicators if indicator in content_upper)
        context_scores["governance"] += min(governance_count * 0.4, 3)
        
        # 8. DADOS DE PERFORMANCE
        performance_indicators = [
            "return", "yield", "gain", "loss", "outperform", "underperform",
            "benchmark", "alpha", "beta", "sharpe", "volatility", "tracking",
            "attribution", "contribution", "drawdown", "recovery"
        ]
        
        performance_count = sum(1 for indicator in performance_indicators if indicator in content_upper)
        context_scores["performance_data"] += min(performance_count * 0.3, 3)
        
        # 9. INFORMAÇÕES DE CONTATO
        contact_patterns = [
            r'www\.[\w\.-]+\.com',           # URLs
            r'[\w\.-]+@[\w\.-]+\.\w+',       # Emails  
            r'\+?\d{1,4}[-.\s]?\d{3,4}[-.\s]?\d{4,6}',  # Telefones
            r'address|contact|phone|email|website|fax'
        ]
        
        contact_matches = sum(1 for pattern in contact_patterns 
                            if re.search(pattern, content, re.IGNORECASE))
        context_scores["contact_info"] += min(contact_matches * 0.5, 2)
        
        # 10. ANÁLISE DE DENSIDADE DE CONTEÚDO
        # Se tem muitos números mas poucos indicadores específicos
        number_density = len(re.findall(r'\b\d+(?:\.\d+)?\b', content)) / max(len(content.split()), 1)
        
        if number_density > 0.1:  # Muitos números
            context_scores["financial_data"] += 1
            context_scores["performance_data"] += 1
        
        # 11. ANÁLISE DE COMPRIMENTO E ESTRUTURA
        if len(content_clean) < 100:
            context_scores["document_header"] += 1
        elif len(content_clean) > 1000:
            context_scores["general_content"] += 1
        
        # 12. DETERMINAR CONTEXTO FINAL
        max_score = max(context_scores.values())
        
        # Se nenhuma categoria tem pontuação significativa
        if max_score < 1:
            return "general_content"
        
        # Retornar categoria com maior pontuação
        for context, score in context_scores.items():
            if score == max_score:
                return context
        
        return "general_content"



    def extract_to_chunks(self, file_path: str, output_dir: str = "C:/extrair") -> Dict[str, Any]:
        """Extração principal para chunks contextuais."""
        
        file_path = Path(file_path)
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"🚀 EXTRAÇÃO PDF PARA CHUNKS CONTEXTUAIS")
        print(f"📄 Arquivo: {file_path.name}")
        print(f"📁 Saída: {output_dir}")
        print(f"📦 Configuração: chunk_size={self.chunk_size}, overlap={self.overlap}")
        
        try:
            doc = fitz.open(str(file_path))
            print(f"📊 Total de páginas: {len(doc)}")
            
            # 1. Extrair metadados do documento
            doc_metadata = self.extract_document_metadata(doc)
            
            # 2. Extrair elementos de todas as páginas
            all_elements = []
            for page_num in range(len(doc)):
                page_elements = self.extract_page_elements(doc, page_num)
                all_elements.append(page_elements)
            
            # 3. Criar chunks contextuais
            content_chunks = self.create_content_chunks(all_elements)
            
            # 4. Estrutura final dos dados
            extracted_data = {
                "document_info": {
                    "filename": file_path.name,
                    "source_path": str(file_path),
                    "metadata": doc_metadata,
                    "extraction_config": {
                        "chunk_size": self.chunk_size,
                        "overlap": self.overlap,
                        "extraction_timestamp": str(datetime.datetime.now())
                    }
                },
                "content_chunks": content_chunks,
                "summary": {
                    "total_chunks": len(content_chunks),
                    "total_pages": len(all_elements),
                    "total_words": sum(chunk["metadata"]["word_count"] for chunk in content_chunks),
                    "total_images": sum(len(page["images"]) for page in all_elements),
                    "total_tables": sum(len(page["tables"]) for page in all_elements),
                    "content_types": list(set(
                        ctype for chunk in content_chunks 
                        for ctype in chunk["metadata"]["content_types"]
                    ))
                },
                "page_elements": all_elements  # Manter referência original se necessário
            }
            
            # 5. Salvar resultado
            output_file = output_dir / f"{file_path.stem}_chunks.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(extracted_data, f, indent=2, ensure_ascii=False)
            
            print(f"\n✅ EXTRAÇÃO CONCLUÍDA!")
            print(f"📊 Estatísticas:")
            print(f" - Chunks criados: {extracted_data['summary']['total_chunks']}")
            print(f" - Palavras totais: {extracted_data['summary']['total_words']:,}")
            print(f" - Imagens detectadas: {extracted_data['summary']['total_images']}")
            print(f" - Tabelas detectadas: {extracted_data['summary']['total_tables']}")
            print(f" - Tipos de conteúdo: {', '.join(extracted_data['summary']['content_types'])}")
            print(f"💾 Arquivo salvo: {output_file}")
            
            doc.close()
            return extracted_data
            
        except Exception as e:
            print(f"❌ Erro na extração: {e}")
            return None


def main():
    """Função principal."""
    print("🚀 Extrator PDF para Chunks Contextuais - LLM Ready")
    
    if len(sys.argv) < 2:
        print("\n💡 COMO USAR:")
        print(" python pdf_extractor.py <arquivo.pdf>                    # Extração em chunks")
        print(" python pdf_extractor.py <arquivo.pdf> <chunk_size>       # Com tamanho customizado")
        print(" python pdf_extractor.py <arquivo.pdf> <chunk_size> <overlap> # Personalizado completo")
        print("\nExemplos:")
        print(" python pdf_extractor.py documento.pdf")
        print(" python pdf_extractor.py documento.pdf 1500             # chunks de 1500 chars")
        print(" python pdf_extractor.py documento.pdf 1200 300         # chunks 1200, overlap 300")
        return

    pdf_file = sys.argv[1]
    chunk_size = int(sys.argv[2]) if len(sys.argv) > 2 else 1000
    overlap = int(sys.argv[3]) if len(sys.argv) > 3 else 200
    
    if not Path(pdf_file).exists():
        print(f"❌ Arquivo não encontrado: {pdf_file}")
        return
    
    # Criar extrator e executar
    extractor = PDFToChunksExtractor(chunk_size=chunk_size, overlap=overlap)
    result = extractor.extract_to_chunks(pdf_file)
    
    if result:
        print(f"\n🎯 RESULTADO OTIMIZADO PARA LLM!")
        print(f"📦 {result['summary']['total_chunks']} chunks prontos para análise")
        print(f"💡 Cada chunk mantém contexto com chunks anteriores/posteriores")


if __name__ == "__main__":
    print("\n💡 EXEMPLO PARA SEU ARQUIVO:")
    print(r" python pdf_extractor.py C:\extrair\paginas.pdf")
    print()
    main()