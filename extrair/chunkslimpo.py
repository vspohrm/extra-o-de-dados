#!/usr/bin/env python3
"""
Extrator de PDF para Chunks de Contexto - Versão com Integração SQL
"""

import json
import sys
import re
from pathlib import Path
import datetime
import pyodbc
import pandas as pd
from typing import List, Dict, Any, Optional

# Verificar PyMuPDF
try:
    import fitz
    print("✅ PyMuPDF disponível")
except ImportError:
    print("❌ Execute: pip install PyMuPDF")
    sys.exit(1)


class PDFToChunksExtractor:
    """Extrator de PDF em chunks contextuais para LLM com integração SQL."""

    def __init__(self, chunk_size: int = 1000, overlap: int = 200):
        self.chunk_size = chunk_size
        self.overlap = overlap
        self.min_chunk_size = 100
    
    @staticmethod
    def get_data_from_sql(query):
        """Função para puxar tabela da base de dados - versão corrigida."""
        try:
            # Usar SQLAlchemy para evitar warnings
            from sqlalchemy import create_engine
            import urllib
            
            # Configurar conexão SQLAlchemy
            params = urllib.parse.quote_plus(
                'Driver={SQL Server};'
                'Server=sql.msquare.local;'
                'Database=M_SQUARE_Prod;'
                'Trusted_Connection=yes;'
            )
            engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
            
            return pd.read_sql(query, engine)
            
        except ImportError:
            # Fallback para pyodbc se SQLAlchemy não estiver disponível
            print("⚠️ SQLAlchemy não disponível, usando pyodbc (pode gerar warnings)")
            try:
                cnxn = pyodbc.connect('Driver={SQL Server};'
                                    'Server=sql.msquare.local;'
                                    'Database=M_SQUARE_Prod;'
                                    'Trusted_Connection=yes;')
                return pd.read_sql(query, cnxn)
            except Exception as e:
                print(f"❌ Erro ao conectar com SQL: {e}")
                return None
        except Exception as e:
            print(f"❌ Erro ao conectar com SQL: {e}")
            return None
    
    @staticmethod
    def get_fund_info_from_sql(fund_identifier: str = None, map_id: int = None) -> Dict[str, Any]:
        """Busca informações do fundo no banco de dados usando MapID ou identificador."""
        
        # Query base
        query = """
        SELECT MapID, MgtCompany, FundName, FundShortName, AssetClassReport, Return_Currency 
        FROM Core.Tbl_Dim_FundInformationMapping
        """
        
        # Priorizar MapID se fornecido
        if map_id:
            query += f" WHERE MapID = {map_id}"
            print(f"🔍 Buscando por MapID: {map_id}")
        elif fund_identifier:
            # Busca por texto nos campos principais
            query += f"""
            WHERE FundName LIKE '%{fund_identifier}%' 
               OR FundShortName LIKE '%{fund_identifier}%'
               OR MgtCompany LIKE '%{fund_identifier}%'
            """
            print(f"🔍 Buscando por identificador: {fund_identifier}")
        else:
            print("🔍 Buscando todos os fundos disponíveis")
        
        print(f"🔍 Executando query SQL...")
        df = PDFToChunksExtractor.get_data_from_sql(query)
        
        if df is None or df.empty:
            print("⚠️ Nenhum resultado encontrado no SQL")
            return {
                "sql_data_available": False,
                "error": "No data found in database",
                "search_criteria": {"map_id": map_id, "fund_identifier": fund_identifier}
            }
        
        print(f"📊 Encontrados {len(df)} registros no SQL")
        
        # Se encontrou múltiplos registros
        if len(df) > 1:
            print(f"📋 Múltiplos fundos encontrados:")
            for idx, row in df.head(10).iterrows():  # Mostrar até 10
                print(f"  MapID {row['MapID']}: {row['FundName']} - {row['MgtCompany']}")
            
            # Se não usou MapID, sugerir usar MapID específico
            if not map_id:
                print(f"💡 Para selecionar um fundo específico, use: map_id={df.iloc[0]['MapID']}")
            
            # Por padrão, pegar o primeiro
            selected_row = df.iloc[0]
            print(f"✅ Selecionado automaticamente: MapID {selected_row['MapID']} - {selected_row['FundName']}")
        else:
            selected_row = df.iloc[0]
            print(f"✅ Encontrado: MapID {selected_row['MapID']} - {selected_row['FundName']}")
        
        # Retornar dados estruturados
        return {
            "sql_data_available": True,
            "map_id": int(selected_row['MapID']),  # Garantir que é int
            "management_company": selected_row['MgtCompany'],
            "fund_name": selected_row['FundName'],
            "fund_short_name": selected_row['FundShortName'],
            "asset_class_report": selected_row['AssetClassReport'],
            "return_currency": selected_row['Return_Currency'],
            "query_used": query,
            "total_records_found": len(df),
            "search_criteria": {"map_id": map_id, "fund_identifier": fund_identifier}
        }
    
    @staticmethod
    def extract_fund_identifier_from_pdf(doc) -> Optional[str]:
        """Tenta extrair identificador do fundo do próprio PDF."""
        
        # Tentar extrair das primeiras páginas
        fund_identifiers = []
        
        for page_num in range(min(3, len(doc))):  # Primeiras 3 páginas
            page = doc[page_num]
            text = page.get_text()
            
            # Padrões comuns de nomes de fundo
            patterns = [
                r'([A-Z][a-zA-Z\s]+(?:Fund|Holdings|Capital|Partners|Investment|Management)[\s\w]*)',
                r'([A-Z][a-zA-Z\s]+(?:Ltd|LLC|Inc|Corp|LP|Limited))',
                r'Fund Name[:\s]+([A-Za-z\s]+)',
                r'Company[:\s]+([A-Za-z\s]+)',
            ]
            
            for pattern in patterns:
                matches = re.findall(pattern, text)
                fund_identifiers.extend(matches)
        
        # Limpar e retornar mais provável
        if fund_identifiers:
            # Pegar o mais comum ou primeiro
            identifier = fund_identifiers[0].strip()
            print(f"🎯 Identificador extraído do PDF: {identifier}")
            return identifier
        
        return None
    
    @staticmethod
    def extract_document_metadata(doc, fund_identifier: str = None, map_id: int = None) -> Dict[str, Any]:
        """Extrai metadados do documento com integração SQL."""
        
        try:
            # Metadados básicos do PDF
            metadata = doc.metadata
            basic_metadata = {
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
            basic_metadata = {
                "error": str(e),
                "total_pages": len(doc),
                "extraction_timestamp": str(datetime.datetime.now())
            }
        
        # Priorizar MapID, depois fund_identifier, depois auto-detectar
        search_id = map_id
        search_text = fund_identifier
        
        if not search_id and not search_text:
            search_text = PDFToChunksExtractor.extract_fund_identifier_from_pdf(doc)
        
        # Buscar informações no SQL
        print("💾 Buscando informações do fundo no SQL...")
        sql_info = PDFToChunksExtractor.get_fund_info_from_sql(search_text, search_id)
        
        # Combinar metadados
        combined_metadata = {
            **basic_metadata,
            "fund_database_info": sql_info
        }
        
        return combined_metadata

    def extract_page_elements(self, doc, page_num: int) -> Dict[str, Any]:
        """Extrai todos os elementos de uma página."""
        page = doc[page_num]
        print(f"🔍 Analisando página {page_num + 1}")
        
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
            # Aplicar limpeza ao texto bruto
            cleaned_text = self.clean_extracted_text(raw_text)
            elements["text_content"] = cleaned_text
            print(f"Texto extraído: {len(cleaned_text.split())} palavras (após limpeza)")
        except Exception as e:
            print(f"Erro na extração de texto: {e}")
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
                                font_info.append({
                                    "font": span.get("font", ""),
                                    "size": round(span.get("size", 0), 1),
                                    "flags": span.get("flags", 0),
                                    "color": span.get("color", 0)
                                })
                    
                    if block_text.strip():
                        # Limpar o conteúdo do bloco
                        cleaned_block_content = self.clean_block_content(block_text)
                        
                        # Verificar se deve pular o bloco por ser inútil
                        if self.should_skip_block(cleaned_block_content):
                            print(f"Pulando bloco inútil: '{block_text[:50]}...'")
                            continue
                        
                        block_type = self._classify_block_type(cleaned_block_content, font_info)
                        
                        structured_block = {
                            "id": f"page_{page_num + 1}_block_{i + 1}",
                            "type": block_type,
                            "content": cleaned_block_content,
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

        # 3. DETECTAR IMAGENS
        try:
            images = page.get_images(full=True)
            elements["images"] = []
            
            for i, img in enumerate(images):
                try:
                    img_data = doc.extract_image(img[0])
                    
                    image_info = {
                        "id": f"page_{page_num + 1}_image_{i + 1}",
                        "dimensions": f"{img_data['width']}x{img_data['height']}",
                        "size_bytes": len(img_data["image"]),
                        "format": img_data["ext"],
                        "colorspace": img_data["colorspace"],
                        "position_ref": img[0],
                        "likely_type": self._classify_image_type(img_data)
                    }
                    
                    elements["images"].append(image_info)
                    
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

    @staticmethod
    def clean_extracted_text(text: str) -> str:
        """Remove informações inúteis e limpa o texto extraído."""
        if not text or not text.strip():
            return ""
        
        # 1. Normalizar espaços em branco
        text = re.sub(r'\s+', ' ', text.strip())
        
        # 2. Remover sequências de caracteres repetitivos inúteis
        patterns_to_clean = [
            r'-{3,}',                    # --- ou mais traços
            r'_{3,}',                    # ___ ou mais underscores
            r'\.{3,}',                   # ... ou mais pontos
            r'={3,}',                    # === ou mais iguais
            r'\*{3,}',                   # *** ou mais asteriscos
            r'#{3,}',                    # ### ou mais hashtags
            r'\+{3,}',                   # +++ ou mais plus
            r'~{3,}',                    # ~~~ ou mais til
            r'`{3,}',                    # ``` ou mais backticks
        ]ores
            r'\.{3,}',                   # ... ou mais pontos
            r'={3,}',                    # === ou mais iguais
            r'\*{3,}',                   # *** ou mais asteriscos
            r'#{3,}',                    # ### ou mais hashtags
            r'\+{3,}',                   # +++ ou mais plus
            r'~{3,}',                    # ~~~ ou mais til
            r'`{3,}',                    # ``` ou mais backticks
        ]
        
        for pattern in patterns_to_clean:
            text = re.sub(pattern, '', text)
        
        # 3. Remover linhas de separação comuns
        separation_patterns = [
            r'^\s*[-_=*+~#]{1,}\s*
        """Classifica o tipo de bloco baseado no conteúdo e formatação."""
        text_upper = text.upper()
        
        if any(keyword in text_upper for keyword in 
               ["CONFIDENTIAL", "MEMORANDUM", "FUND", "NOTICE", "REGULATORY"]):
            return "heading"
        
        if re.match(r'^\s*[-•▪▫]\s+', text) or re.match(r'^\s*\d+\.\s+', text):
            return "list_item"
        
        if len(re.findall(r'\b\d+\b', text)) > 3 and ('\t' in text or '  ' in text):
            return "table_data"
        
        if len(text) < 200 and (text.startswith('*') or text.startswith('Note:')):
            return "footnote"
        
        if len(text) > 100:
            return "paragraph"
        
        return "text_block"

    def _classify_image_type(self, img_data: Dict) -> str:
        """Classifica o tipo de imagem baseado nas características."""
        width = img_data.get("width", 0)
        height = img_data.get("height", 0)
        pixels = width * height
        aspect_ratio = width / height if height > 0 else 1
        
        if (pixels > 50000 and 
            0.5 <= aspect_ratio <= 3.0 and
            width > 300 and height > 200):
            return "chart"
        
        if pixels > 20000 and aspect_ratio > 2.5:
            return "diagram"
        
        if pixels < 20000:
            return "icon"
        
        return "image"

    def _detect_tables(self, text: str) -> List[Dict]:
        """Detecta tabelas no texto com limpeza prévia."""
        if not text:
            return []
        
        # Aplicar limpeza antes de detectar tabelas
        clean_text = self.clean_extracted_text(text)
        tables = []
        lines = clean_text.split('\n')
        
        current_table = []
        for i, line in enumerate(lines):
            line = line.strip()
            
            # Pular linhas que são claramente ruído
            if self.should_skip_block(line):
                continue
            
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
        print(f"📦 Criando chunks de conteúdo...")
        
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
        
        for page_elements in all_elements:
            page_num = page_elements["page_number"]
            print(f"📄 Processando página {page_num} para chunks...")
            
            visual_context = ""
            if page_elements["images"]:
                visual_context += f"\n[PÁGINA {page_num} CONTÉM {len(page_elements['images'])} IMAGENS]"
                for img in page_elements["images"]:
                    if img.get("likely_type") in ["chart", "graph"]:
                        visual_context += f"\n[GRÁFICO: {img['dimensions']}]"
            
            if page_elements["tables"]:
                visual_context += f"\n[PÁGINA {page_num} CONTÉM {len(page_elements['tables'])} TABELAS]"
            
            for block in page_elements["structured_blocks"]:
                block_content = block["content"]
                block_type = block["type"]
                
                content_with_context = block_content
                if visual_context and block_type in ["heading", "paragraph"]:
                    content_with_context = block_content + visual_context
                    visual_context = ""
                
                if (len(current_chunk["content"]) + len(content_with_context) > self.chunk_size and 
                    len(current_chunk["content"]) > self.min_chunk_size):
                    
                    self._finalize_chunk(current_chunk, chunk_counter)
                    chunks.append(current_chunk)
                    
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
                
                if current_chunk["content"]:
                    current_chunk["content"] += "\n\n"
                current_chunk["content"] += content_with_context
                
                if page_num not in current_chunk["metadata"]["pages"]:
                    current_chunk["metadata"]["pages"].append(page_num)
                current_chunk["metadata"]["elements"].append(block["id"])
                if block_type not in current_chunk["metadata"]["content_types"]:
                    current_chunk["metadata"]["content_types"].append(block_type)
        
        if current_chunk["content"].strip():
            self._finalize_chunk(current_chunk, chunk_counter)
            chunks.append(current_chunk)
        
        print(f"✅ Criados {len(chunks)} chunks de conteúdo")
        
        for i, chunk in enumerate(chunks):
            chunk["context"]["chunk_position"] = f"{i + 1}/{len(chunks)}"
            if i > 0:
                chunk["context"]["previous_chunk_id"] = chunks[i - 1]["id"]
            if i < len(chunks) - 1:
                chunk["context"]["next_chunk_id"] = chunks[i + 1]["id"]
        
        return chunks

    def _finalize_chunk(self, chunk: Dict, chunk_id: int):
        """Finaliza um chunk calculando metadados."""
        chunk["id"] = f"chunk_{chunk_id}"
        chunk["metadata"]["word_count"] = len(chunk["content"].split())
        chunk["metadata"]["char_count"] = len(chunk["content"])

    def _get_overlap_content(self, content: str) -> str:
        """Obtém conteúdo de sobreposição do chunk anterior."""
        if len(content) <= self.overlap:
            return content
        
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
        
        summary = ' '.join(words[:10]) + " ... " + ' '.join(words[-10:])
        return summary

    def _get_section_context(self, content: str) -> str:
        """Identifica o contexto da seção atual."""
        content_upper = content.upper()
        
        if any(keyword in content_upper for keyword in ["CONFIDENTIAL", "MEMORANDUM"]):
            return "document_header"
        elif any(keyword in content_upper for keyword in ["RISK", "WARNING", "CAUTION"]):
            return "risk_section"
        elif any(keyword in content_upper for keyword in ["INVESTMENT", "FUND", "PORTFOLIO"]):
            return "investment_section"
        elif any(keyword in content_upper for keyword in ["LEGAL", "REGULATORY", "COMPLIANCE"]):
            return "legal_section"
        elif re.search(r'\d+\.\d+%|\$\d+|USD|EUR', content):
            return "financial_data"
        else:
            return "general_content"
    
    def extract_to_chunks(self, file_path: str, output_dir: str = "C:/extrair", 
                         fund_identifier: str = None, map_id: int = None) -> Dict[str, Any]:
        """Extração principal para chunks contextuais com dados SQL."""
        
        file_path = Path(file_path)
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"EXTRAÇÃO PDF PARA CHUNKS CONTEXTUAIS COM SQL")
        print(f"Arquivo: {file_path.name}")
        print(f"Saída: {output_dir}")
        
        if map_id:
            print(f"MapID: {map_id}")
        elif fund_identifier:
            print(f"Identificador do fundo: {fund_identifier}")
        else:
            print(f"Identificador do fundo: Auto-detectar")
        
        try:
            doc = fitz.open(str(file_path))
            print(f"Total de páginas: {len(doc)}")
            
            # 1. Extrair metadados com integração SQL
            doc_metadata = self.extract_document_metadata(doc, fund_identifier, map_id)
            
            # 2. Extrair elementos de todas as páginas
            all_elements = []
            for page_num in range(len(doc)):
                page_elements = self.extract_page_elements(doc, page_num)
                all_elements.append(page_elements)
            
            # 3. Criar chunks contextuais
            content_chunks = self.create_content_chunks(all_elements)
            
            # 4. Estrutura final dos dados com informações SQL
            extracted_data = {
                "document_info": {
                    "filename": file_path.name,
                    "source_path": str(file_path),
                    "metadata": doc_metadata,
                    "extraction_config": {
                        "chunk_size": self.chunk_size,
                        "overlap": self.overlap,
                        "extraction_timestamp": str(datetime.datetime.now()),
                        "fund_identifier_used": fund_identifier,
                        "map_id_used": map_id
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
                "page_elements": all_elements
            }
            
            # 5. Salvar resultado
            output_file = output_dir / f"{file_path.stem}_chunks.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(extracted_data, f, indent=2, ensure_ascii=False)
            
            # 6. Mostrar resumo com dados SQL
            print(f"\nEXTRAÇÃO CONCLUÍDA!")
            print(f"Estatísticas:")
            print(f" - Chunks criados: {extracted_data['summary']['total_chunks']}")
            print(f" - Palavras totais: {extracted_data['summary']['total_words']:,}")
            print(f" - Imagens detectadas: {extracted_data['summary']['total_images']}")
            print(f" - Tabelas detectadas: {extracted_data['summary']['total_tables']}")
            
            # Mostrar informações SQL se disponível
            sql_info = doc_metadata.get("fund_database_info", {})
            if sql_info.get("sql_data_available", False):
                print(f"\nINFORMAÇÕES DO FUNDO (SQL):")
                print(f" - MapID: {sql_info.get('map_id', 'N/A')}")
                print(f" - Gestor: {sql_info.get('management_company', 'N/A')}")
                print(f" - Fundo: {sql_info.get('fund_name', 'N/A')}")
                print(f" - Nome curto: {sql_info.get('fund_short_name', 'N/A')}")
                print(f" - Asset class: {sql_info.get('asset_class_report', 'N/A')}")
                print(f" - Moeda: {sql_info.get('return_currency', 'N/A')}")
            else:
                print(f"\nInformações SQL não disponíveis")
                if sql_info.get("total_records_found", 0) > 1:
                    print(f"Sugestão: Use MapID específico para seleção precisa")
            
            print(f"\nArquivo salvo: {output_file}")
            
            doc.close()
            return extracted_data
            
        except Exception as e:
            print(f"Erro na extração: {e}")
            return None


def main():
    """Função principal com suporte a MapID e identificador de fundo."""
    print("Extrator PDF para Chunks Contextuais - Com Integração SQL")
    
    if len(sys.argv) < 2:
        print("\nCOMO USAR:")
        print(" python pdf_extractor.py <arquivo.pdf>")
        print(" python pdf_extractor.py <arquivo.pdf> <fund_identifier>")
        print(" python pdf_extractor.py <arquivo.pdf> --map-id <MapID>")
        print(" python pdf_extractor.py <arquivo.pdf> --map-id <MapID> <chunk_size>")
        print("\nExemplos:")
        print(" python pdf_extractor.py documento.pdf")
        print(" python pdf_extractor.py documento.pdf 'Pershing Square'")
        print(" python pdf_extractor.py documento.pdf --map-id 123")
        print(" python pdf_extractor.py documento.pdf --map-id 123 1500")
        return

    pdf_file = sys.argv[1]
    fund_identifier = None
    map_id = None
    chunk_size = 1000
    overlap = 200
    
    # Processar argumentos
    i = 2
    while i < len(sys.argv):
        if sys.argv[i] == "--map-id" and i + 1 < len(sys.argv):
            try:
                map_id = int(sys.argv[i + 1])
                i += 2
            except ValueError:
                print(f"Erro: MapID deve ser um número inteiro: {sys.argv[i + 1]}")
                return
        elif sys.argv[i].isdigit() and not map_id:  # chunk_size
            chunk_size = int(sys.argv[i])
            i += 1
        elif not fund_identifier and not map_id:  # fund_identifier
            fund_identifier = sys.argv[i]
            i += 1
        else:
            i += 1
    
    if not Path(pdf_file).exists():
        print(f"Arquivo não encontrado: {pdf_file}")
        return
    
    print(f"\nParametros:")
    print(f" - Arquivo: {pdf_file}")
    if map_id:
        print(f" - MapID: {map_id}")
    elif fund_identifier:
        print(f" - Identificador: {fund_identifier}")
    else:
        print(f" - Detecção automática ativada")
    print(f" - Chunk size: {chunk_size}")
    
    # Criar extrator e executar
    extractor = PDFToChunksExtractor(chunk_size=chunk_size, overlap=overlap)
    result = extractor.extract_to_chunks(pdf_file, fund_identifier=fund_identifier, map_id=map_id)
    
    if result:
        print(f"\nRESULTADO OTIMIZADO PARA LLM!")
        print(f"{result['summary']['total_chunks']} chunks prontos para análise")
        print(f"Cada chunk mantém contexto + informações do fundo do SQL")


if __name__ == "__main__":
    print("\nEXEMPLO PARA SEU ARQUIVO:")
    print(" python pdf_extractor.py C:\\extrair\\paginas.pdf --map-id 123")
    print(" python pdf_extractor.py C:\\extrair\\paginas.pdf 'Pershing Square'")
    print()
    main(),    # Linhas só com caracteres separadores
            r'^\s*Page\s+\d+\s*
        """Classifica o tipo de bloco baseado no conteúdo e formatação."""
        text_upper = text.upper()
        
        if any(keyword in text_upper for keyword in 
               ["CONFIDENTIAL", "MEMORANDUM", "FUND", "NOTICE", "REGULATORY"]):
            return "heading"
        
        if re.match(r'^\s*[-•▪▫]\s+', text) or re.match(r'^\s*\d+\.\s+', text):
            return "list_item"
        
        if len(re.findall(r'\b\d+\b', text)) > 3 and ('\t' in text or '  ' in text):
            return "table_data"
        
        if len(text) < 200 and (text.startswith('*') or text.startswith('Note:')):
            return "footnote"
        
        if len(text) > 100:
            return "paragraph"
        
        return "text_block"

    def _classify_image_type(self, img_data: Dict) -> str:
        """Classifica o tipo de imagem baseado nas características."""
        width = img_data.get("width", 0)
        height = img_data.get("height", 0)
        pixels = width * height
        aspect_ratio = width / height if height > 0 else 1
        
        if (pixels > 50000 and 
            0.5 <= aspect_ratio <= 3.0 and
            width > 300 and height > 200):
            return "chart"
        
        if pixels > 20000 and aspect_ratio > 2.5:
            return "diagram"
        
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
            
            if (re.search(r'\d+.*\d+.*\d+', line) or 
                '\t' in line or 
                len(re.findall(r'\s{3,}', line)) >= 2):
                
                current_table.append({
                    "line_number": i + 1,
                    "content": line
                })
            else:
                if len(current_table) >= 2:
                    tables.append({
                        "id": f"table_{len(tables) + 1}",
                        "start_line": current_table[0]["line_number"],
                        "end_line": current_table[-1]["line_number"],
                        "rows": current_table,
                        "row_count": len(current_table)
                    })
                current_table = []
        
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
        print(f"📦 Criando chunks de conteúdo...")
        
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
        
        for page_elements in all_elements:
            page_num = page_elements["page_number"]
            print(f"📄 Processando página {page_num} para chunks...")
            
            visual_context = ""
            if page_elements["images"]:
                visual_context += f"\n[PÁGINA {page_num} CONTÉM {len(page_elements['images'])} IMAGENS]"
                for img in page_elements["images"]:
                    if img.get("likely_type") in ["chart", "graph"]:
                        visual_context += f"\n[GRÁFICO: {img['dimensions']}]"
            
            if page_elements["tables"]:
                visual_context += f"\n[PÁGINA {page_num} CONTÉM {len(page_elements['tables'])} TABELAS]"
            
            for block in page_elements["structured_blocks"]:
                block_content = block["content"]
                block_type = block["type"]
                
                content_with_context = block_content
                if visual_context and block_type in ["heading", "paragraph"]:
                    content_with_context = block_content + visual_context
                    visual_context = ""
                
                if (len(current_chunk["content"]) + len(content_with_context) > self.chunk_size and 
                    len(current_chunk["content"]) > self.min_chunk_size):
                    
                    self._finalize_chunk(current_chunk, chunk_counter)
                    chunks.append(current_chunk)
                    
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
                
                if current_chunk["content"]:
                    current_chunk["content"] += "\n\n"
                current_chunk["content"] += content_with_context
                
                if page_num not in current_chunk["metadata"]["pages"]:
                    current_chunk["metadata"]["pages"].append(page_num)
                current_chunk["metadata"]["elements"].append(block["id"])
                if block_type not in current_chunk["metadata"]["content_types"]:
                    current_chunk["metadata"]["content_types"].append(block_type)
        
        if current_chunk["content"].strip():
            self._finalize_chunk(current_chunk, chunk_counter)
            chunks.append(current_chunk)
        
        print(f"✅ Criados {len(chunks)} chunks de conteúdo")
        
        for i, chunk in enumerate(chunks):
            chunk["context"]["chunk_position"] = f"{i + 1}/{len(chunks)}"
            if i > 0:
                chunk["context"]["previous_chunk_id"] = chunks[i - 1]["id"]
            if i < len(chunks) - 1:
                chunk["context"]["next_chunk_id"] = chunks[i + 1]["id"]
        
        return chunks

    def _finalize_chunk(self, chunk: Dict, chunk_id: int):
        """Finaliza um chunk calculando metadados."""
        chunk["id"] = f"chunk_{chunk_id}"
        chunk["metadata"]["word_count"] = len(chunk["content"].split())
        chunk["metadata"]["char_count"] = len(chunk["content"])

    def _get_overlap_content(self, content: str) -> str:
        """Obtém conteúdo de sobreposição do chunk anterior."""
        if len(content) <= self.overlap:
            return content
        
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
        
        summary = ' '.join(words[:10]) + " ... " + ' '.join(words[-10:])
        return summary

    def _get_section_context(self, content: str) -> str:
        """Identifica o contexto da seção atual."""
        content_upper = content.upper()
        
        if any(keyword in content_upper for keyword in ["CONFIDENTIAL", "MEMORANDUM"]):
            return "document_header"
        elif any(keyword in content_upper for keyword in ["RISK", "WARNING", "CAUTION"]):
            return "risk_section"
        elif any(keyword in content_upper for keyword in ["INVESTMENT", "FUND", "PORTFOLIO"]):
            return "investment_section"
        elif any(keyword in content_upper for keyword in ["LEGAL", "REGULATORY", "COMPLIANCE"]):
            return "legal_section"
        elif re.search(r'\d+\.\d+%|\$\d+|USD|EUR', content):
            return "financial_data"
        else:
            return "general_content"
    
    def extract_to_chunks(self, file_path: str, output_dir: str = "C:/extrair", 
                         fund_identifier: str = None, map_id: int = None) -> Dict[str, Any]:
        """Extração principal para chunks contextuais com dados SQL."""
        
        file_path = Path(file_path)
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"EXTRAÇÃO PDF PARA CHUNKS CONTEXTUAIS COM SQL")
        print(f"Arquivo: {file_path.name}")
        print(f"Saída: {output_dir}")
        
        if map_id:
            print(f"MapID: {map_id}")
        elif fund_identifier:
            print(f"Identificador do fundo: {fund_identifier}")
        else:
            print(f"Identificador do fundo: Auto-detectar")
        
        try:
            doc = fitz.open(str(file_path))
            print(f"Total de páginas: {len(doc)}")
            
            # 1. Extrair metadados com integração SQL
            doc_metadata = self.extract_document_metadata(doc, fund_identifier, map_id)
            
            # 2. Extrair elementos de todas as páginas
            all_elements = []
            for page_num in range(len(doc)):
                page_elements = self.extract_page_elements(doc, page_num)
                all_elements.append(page_elements)
            
            # 3. Criar chunks contextuais
            content_chunks = self.create_content_chunks(all_elements)
            
            # 4. Estrutura final dos dados com informações SQL
            extracted_data = {
                "document_info": {
                    "filename": file_path.name,
                    "source_path": str(file_path),
                    "metadata": doc_metadata,
                    "extraction_config": {
                        "chunk_size": self.chunk_size,
                        "overlap": self.overlap,
                        "extraction_timestamp": str(datetime.datetime.now()),
                        "fund_identifier_used": fund_identifier,
                        "map_id_used": map_id
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
                "page_elements": all_elements
            }
            
            # 5. Salvar resultado
            output_file = output_dir / f"{file_path.stem}_chunks.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(extracted_data, f, indent=2, ensure_ascii=False)
            
            # 6. Mostrar resumo com dados SQL
            print(f"\nEXTRAÇÃO CONCLUÍDA!")
            print(f"Estatísticas:")
            print(f" - Chunks criados: {extracted_data['summary']['total_chunks']}")
            print(f" - Palavras totais: {extracted_data['summary']['total_words']:,}")
            print(f" - Imagens detectadas: {extracted_data['summary']['total_images']}")
            print(f" - Tabelas detectadas: {extracted_data['summary']['total_tables']}")
            
            # Mostrar informações SQL se disponível
            sql_info = doc_metadata.get("fund_database_info", {})
            if sql_info.get("sql_data_available", False):
                print(f"\nINFORMAÇÕES DO FUNDO (SQL):")
                print(f" - MapID: {sql_info.get('map_id', 'N/A')}")
                print(f" - Gestor: {sql_info.get('management_company', 'N/A')}")
                print(f" - Fundo: {sql_info.get('fund_name', 'N/A')}")
                print(f" - Nome curto: {sql_info.get('fund_short_name', 'N/A')}")
                print(f" - Asset class: {sql_info.get('asset_class_report', 'N/A')}")
                print(f" - Moeda: {sql_info.get('return_currency', 'N/A')}")
            else:
                print(f"\nInformações SQL não disponíveis")
                if sql_info.get("total_records_found", 0) > 1:
                    print(f"Sugestão: Use MapID específico para seleção precisa")
            
            print(f"\nArquivo salvo: {output_file}")
            
            doc.close()
            return extracted_data
            
        except Exception as e:
            print(f"Erro na extração: {e}")
            return None


def main():
    """Função principal com suporte a MapID e identificador de fundo."""
    print("Extrator PDF para Chunks Contextuais - Com Integração SQL")
    
    if len(sys.argv) < 2:
        print("\nCOMO USAR:")
        print(" python pdf_extractor.py <arquivo.pdf>")
        print(" python pdf_extractor.py <arquivo.pdf> <fund_identifier>")
        print(" python pdf_extractor.py <arquivo.pdf> --map-id <MapID>")
        print(" python pdf_extractor.py <arquivo.pdf> --map-id <MapID> <chunk_size>")
        print("\nExemplos:")
        print(" python pdf_extractor.py documento.pdf")
        print(" python pdf_extractor.py documento.pdf 'Pershing Square'")
        print(" python pdf_extractor.py documento.pdf --map-id 123")
        print(" python pdf_extractor.py documento.pdf --map-id 123 1500")
        return

    pdf_file = sys.argv[1]
    fund_identifier = None
    map_id = None
    chunk_size = 1000
    overlap = 200
    
    # Processar argumentos
    i = 2
    while i < len(sys.argv):
        if sys.argv[i] == "--map-id" and i + 1 < len(sys.argv):
            try:
                map_id = int(sys.argv[i + 1])
                i += 2
            except ValueError:
                print(f"Erro: MapID deve ser um número inteiro: {sys.argv[i + 1]}")
                return
        elif sys.argv[i].isdigit() and not map_id:  # chunk_size
            chunk_size = int(sys.argv[i])
            i += 1
        elif not fund_identifier and not map_id:  # fund_identifier
            fund_identifier = sys.argv[i]
            i += 1
        else:
            i += 1
    
    if not Path(pdf_file).exists():
        print(f"Arquivo não encontrado: {pdf_file}")
        return
    
    print(f"\nParametros:")
    print(f" - Arquivo: {pdf_file}")
    if map_id:
        print(f" - MapID: {map_id}")
    elif fund_identifier:
        print(f" - Identificador: {fund_identifier}")
    else:
        print(f" - Detecção automática ativada")
    print(f" - Chunk size: {chunk_size}")
    
    # Criar extrator e executar
    extractor = PDFToChunksExtractor(chunk_size=chunk_size, overlap=overlap)
    result = extractor.extract_to_chunks(pdf_file, fund_identifier=fund_identifier, map_id=map_id)
    
    if result:
        print(f"\nRESULTADO OTIMIZADO PARA LLM!")
        print(f"{result['summary']['total_chunks']} chunks prontos para análise")
        print(f"Cada chunk mantém contexto + informações do fundo do SQL")


if __name__ == "__main__":
    print("\nEXEMPLO PARA SEU ARQUIVO:")
    print(" python pdf_extractor.py C:\\extrair\\paginas.pdf --map-id 123")
    print(" python pdf_extractor.py C:\\extrair\\paginas.pdf 'Pershing Square'")
    print()
    main(),       # "Page 1", "Page 2", etc.
            r'^\s*\d+\s*
        """Classifica o tipo de bloco baseado no conteúdo e formatação."""
        text_upper = text.upper()
        
        if any(keyword in text_upper for keyword in 
               ["CONFIDENTIAL", "MEMORANDUM", "FUND", "NOTICE", "REGULATORY"]):
            return "heading"
        
        if re.match(r'^\s*[-•▪▫]\s+', text) or re.match(r'^\s*\d+\.\s+', text):
            return "list_item"
        
        if len(re.findall(r'\b\d+\b', text)) > 3 and ('\t' in text or '  ' in text):
            return "table_data"
        
        if len(text) < 200 and (text.startswith('*') or text.startswith('Note:')):
            return "footnote"
        
        if len(text) > 100:
            return "paragraph"
        
        return "text_block"

    def _classify_image_type(self, img_data: Dict) -> str:
        """Classifica o tipo de imagem baseado nas características."""
        width = img_data.get("width", 0)
        height = img_data.get("height", 0)
        pixels = width * height
        aspect_ratio = width / height if height > 0 else 1
        
        if (pixels > 50000 and 
            0.5 <= aspect_ratio <= 3.0 and
            width > 300 and height > 200):
            return "chart"
        
        if pixels > 20000 and aspect_ratio > 2.5:
            return "diagram"
        
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
            
            if (re.search(r'\d+.*\d+.*\d+', line) or 
                '\t' in line or 
                len(re.findall(r'\s{3,}', line)) >= 2):
                
                current_table.append({
                    "line_number": i + 1,
                    "content": line
                })
            else:
                if len(current_table) >= 2:
                    tables.append({
                        "id": f"table_{len(tables) + 1}",
                        "start_line": current_table[0]["line_number"],
                        "end_line": current_table[-1]["line_number"],
                        "rows": current_table,
                        "row_count": len(current_table)
                    })
                current_table = []
        
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
        print(f"📦 Criando chunks de conteúdo...")
        
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
        
        for page_elements in all_elements:
            page_num = page_elements["page_number"]
            print(f"📄 Processando página {page_num} para chunks...")
            
            visual_context = ""
            if page_elements["images"]:
                visual_context += f"\n[PÁGINA {page_num} CONTÉM {len(page_elements['images'])} IMAGENS]"
                for img in page_elements["images"]:
                    if img.get("likely_type") in ["chart", "graph"]:
                        visual_context += f"\n[GRÁFICO: {img['dimensions']}]"
            
            if page_elements["tables"]:
                visual_context += f"\n[PÁGINA {page_num} CONTÉM {len(page_elements['tables'])} TABELAS]"
            
            for block in page_elements["structured_blocks"]:
                block_content = block["content"]
                block_type = block["type"]
                
                content_with_context = block_content
                if visual_context and block_type in ["heading", "paragraph"]:
                    content_with_context = block_content + visual_context
                    visual_context = ""
                
                if (len(current_chunk["content"]) + len(content_with_context) > self.chunk_size and 
                    len(current_chunk["content"]) > self.min_chunk_size):
                    
                    self._finalize_chunk(current_chunk, chunk_counter)
                    chunks.append(current_chunk)
                    
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
                
                if current_chunk["content"]:
                    current_chunk["content"] += "\n\n"
                current_chunk["content"] += content_with_context
                
                if page_num not in current_chunk["metadata"]["pages"]:
                    current_chunk["metadata"]["pages"].append(page_num)
                current_chunk["metadata"]["elements"].append(block["id"])
                if block_type not in current_chunk["metadata"]["content_types"]:
                    current_chunk["metadata"]["content_types"].append(block_type)
        
        if current_chunk["content"].strip():
            self._finalize_chunk(current_chunk, chunk_counter)
            chunks.append(current_chunk)
        
        print(f"✅ Criados {len(chunks)} chunks de conteúdo")
        
        for i, chunk in enumerate(chunks):
            chunk["context"]["chunk_position"] = f"{i + 1}/{len(chunks)}"
            if i > 0:
                chunk["context"]["previous_chunk_id"] = chunks[i - 1]["id"]
            if i < len(chunks) - 1:
                chunk["context"]["next_chunk_id"] = chunks[i + 1]["id"]
        
        return chunks

    def _finalize_chunk(self, chunk: Dict, chunk_id: int):
        """Finaliza um chunk calculando metadados."""
        chunk["id"] = f"chunk_{chunk_id}"
        chunk["metadata"]["word_count"] = len(chunk["content"].split())
        chunk["metadata"]["char_count"] = len(chunk["content"])

    def _get_overlap_content(self, content: str) -> str:
        """Obtém conteúdo de sobreposição do chunk anterior."""
        if len(content) <= self.overlap:
            return content
        
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
        
        summary = ' '.join(words[:10]) + " ... " + ' '.join(words[-10:])
        return summary

    def _get_section_context(self, content: str) -> str:
        """Identifica o contexto da seção atual."""
        content_upper = content.upper()
        
        if any(keyword in content_upper for keyword in ["CONFIDENTIAL", "MEMORANDUM"]):
            return "document_header"
        elif any(keyword in content_upper for keyword in ["RISK", "WARNING", "CAUTION"]):
            return "risk_section"
        elif any(keyword in content_upper for keyword in ["INVESTMENT", "FUND", "PORTFOLIO"]):
            return "investment_section"
        elif any(keyword in content_upper for keyword in ["LEGAL", "REGULATORY", "COMPLIANCE"]):
            return "legal_section"
        elif re.search(r'\d+\.\d+%|\$\d+|USD|EUR', content):
            return "financial_data"
        else:
            return "general_content"
    
    def extract_to_chunks(self, file_path: str, output_dir: str = "C:/extrair", 
                         fund_identifier: str = None, map_id: int = None) -> Dict[str, Any]:
        """Extração principal para chunks contextuais com dados SQL."""
        
        file_path = Path(file_path)
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"EXTRAÇÃO PDF PARA CHUNKS CONTEXTUAIS COM SQL")
        print(f"Arquivo: {file_path.name}")
        print(f"Saída: {output_dir}")
        
        if map_id:
            print(f"MapID: {map_id}")
        elif fund_identifier:
            print(f"Identificador do fundo: {fund_identifier}")
        else:
            print(f"Identificador do fundo: Auto-detectar")
        
        try:
            doc = fitz.open(str(file_path))
            print(f"Total de páginas: {len(doc)}")
            
            # 1. Extrair metadados com integração SQL
            doc_metadata = self.extract_document_metadata(doc, fund_identifier, map_id)
            
            # 2. Extrair elementos de todas as páginas
            all_elements = []
            for page_num in range(len(doc)):
                page_elements = self.extract_page_elements(doc, page_num)
                all_elements.append(page_elements)
            
            # 3. Criar chunks contextuais
            content_chunks = self.create_content_chunks(all_elements)
            
            # 4. Estrutura final dos dados com informações SQL
            extracted_data = {
                "document_info": {
                    "filename": file_path.name,
                    "source_path": str(file_path),
                    "metadata": doc_metadata,
                    "extraction_config": {
                        "chunk_size": self.chunk_size,
                        "overlap": self.overlap,
                        "extraction_timestamp": str(datetime.datetime.now()),
                        "fund_identifier_used": fund_identifier,
                        "map_id_used": map_id
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
                "page_elements": all_elements
            }
            
            # 5. Salvar resultado
            output_file = output_dir / f"{file_path.stem}_chunks.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(extracted_data, f, indent=2, ensure_ascii=False)
            
            # 6. Mostrar resumo com dados SQL
            print(f"\nEXTRAÇÃO CONCLUÍDA!")
            print(f"Estatísticas:")
            print(f" - Chunks criados: {extracted_data['summary']['total_chunks']}")
            print(f" - Palavras totais: {extracted_data['summary']['total_words']:,}")
            print(f" - Imagens detectadas: {extracted_data['summary']['total_images']}")
            print(f" - Tabelas detectadas: {extracted_data['summary']['total_tables']}")
            
            # Mostrar informações SQL se disponível
            sql_info = doc_metadata.get("fund_database_info", {})
            if sql_info.get("sql_data_available", False):
                print(f"\nINFORMAÇÕES DO FUNDO (SQL):")
                print(f" - MapID: {sql_info.get('map_id', 'N/A')}")
                print(f" - Gestor: {sql_info.get('management_company', 'N/A')}")
                print(f" - Fundo: {sql_info.get('fund_name', 'N/A')}")
                print(f" - Nome curto: {sql_info.get('fund_short_name', 'N/A')}")
                print(f" - Asset class: {sql_info.get('asset_class_report', 'N/A')}")
                print(f" - Moeda: {sql_info.get('return_currency', 'N/A')}")
            else:
                print(f"\nInformações SQL não disponíveis")
                if sql_info.get("total_records_found", 0) > 1:
                    print(f"Sugestão: Use MapID específico para seleção precisa")
            
            print(f"\nArquivo salvo: {output_file}")
            
            doc.close()
            return extracted_data
            
        except Exception as e:
            print(f"Erro na extração: {e}")
            return None


def main():
    """Função principal com suporte a MapID e identificador de fundo."""
    print("Extrator PDF para Chunks Contextuais - Com Integração SQL")
    
    if len(sys.argv) < 2:
        print("\nCOMO USAR:")
        print(" python pdf_extractor.py <arquivo.pdf>")
        print(" python pdf_extractor.py <arquivo.pdf> <fund_identifier>")
        print(" python pdf_extractor.py <arquivo.pdf> --map-id <MapID>")
        print(" python pdf_extractor.py <arquivo.pdf> --map-id <MapID> <chunk_size>")
        print("\nExemplos:")
        print(" python pdf_extractor.py documento.pdf")
        print(" python pdf_extractor.py documento.pdf 'Pershing Square'")
        print(" python pdf_extractor.py documento.pdf --map-id 123")
        print(" python pdf_extractor.py documento.pdf --map-id 123 1500")
        return

    pdf_file = sys.argv[1]
    fund_identifier = None
    map_id = None
    chunk_size = 1000
    overlap = 200
    
    # Processar argumentos
    i = 2
    while i < len(sys.argv):
        if sys.argv[i] == "--map-id" and i + 1 < len(sys.argv):
            try:
                map_id = int(sys.argv[i + 1])
                i += 2
            except ValueError:
                print(f"Erro: MapID deve ser um número inteiro: {sys.argv[i + 1]}")
                return
        elif sys.argv[i].isdigit() and not map_id:  # chunk_size
            chunk_size = int(sys.argv[i])
            i += 1
        elif not fund_identifier and not map_id:  # fund_identifier
            fund_identifier = sys.argv[i]
            i += 1
        else:
            i += 1
    
    if not Path(pdf_file).exists():
        print(f"Arquivo não encontrado: {pdf_file}")
        return
    
    print(f"\nParametros:")
    print(f" - Arquivo: {pdf_file}")
    if map_id:
        print(f" - MapID: {map_id}")
    elif fund_identifier:
        print(f" - Identificador: {fund_identifier}")
    else:
        print(f" - Detecção automática ativada")
    print(f" - Chunk size: {chunk_size}")
    
    # Criar extrator e executar
    extractor = PDFToChunksExtractor(chunk_size=chunk_size, overlap=overlap)
    result = extractor.extract_to_chunks(pdf_file, fund_identifier=fund_identifier, map_id=map_id)
    
    if result:
        print(f"\nRESULTADO OTIMIZADO PARA LLM!")
        print(f"{result['summary']['total_chunks']} chunks prontos para análise")
        print(f"Cada chunk mantém contexto + informações do fundo do SQL")


if __name__ == "__main__":
    print("\nEXEMPLO PARA SEU ARQUIVO:")
    print(" python pdf_extractor.py C:\\extrair\\paginas.pdf --map-id 123")
    print(" python pdf_extractor.py C:\\extrair\\paginas.pdf 'Pershing Square'")
    print()
    main(),              # Linhas só com números (páginas)
            r'^\s*[A-Za-z]\s*
        """Classifica o tipo de bloco baseado no conteúdo e formatação."""
        text_upper = text.upper()
        
        if any(keyword in text_upper for keyword in 
               ["CONFIDENTIAL", "MEMORANDUM", "FUND", "NOTICE", "REGULATORY"]):
            return "heading"
        
        if re.match(r'^\s*[-•▪▫]\s+', text) or re.match(r'^\s*\d+\.\s+', text):
            return "list_item"
        
        if len(re.findall(r'\b\d+\b', text)) > 3 and ('\t' in text or '  ' in text):
            return "table_data"
        
        if len(text) < 200 and (text.startswith('*') or text.startswith('Note:')):
            return "footnote"
        
        if len(text) > 100:
            return "paragraph"
        
        return "text_block"

    def _classify_image_type(self, img_data: Dict) -> str:
        """Classifica o tipo de imagem baseado nas características."""
        width = img_data.get("width", 0)
        height = img_data.get("height", 0)
        pixels = width * height
        aspect_ratio = width / height if height > 0 else 1
        
        if (pixels > 50000 and 
            0.5 <= aspect_ratio <= 3.0 and
            width > 300 and height > 200):
            return "chart"
        
        if pixels > 20000 and aspect_ratio > 2.5:
            return "diagram"
        
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
            
            if (re.search(r'\d+.*\d+.*\d+', line) or 
                '\t' in line or 
                len(re.findall(r'\s{3,}', line)) >= 2):
                
                current_table.append({
                    "line_number": i + 1,
                    "content": line
                })
            else:
                if len(current_table) >= 2:
                    tables.append({
                        "id": f"table_{len(tables) + 1}",
                        "start_line": current_table[0]["line_number"],
                        "end_line": current_table[-1]["line_number"],
                        "rows": current_table,
                        "row_count": len(current_table)
                    })
                current_table = []
        
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
        print(f"📦 Criando chunks de conteúdo...")
        
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
        
        for page_elements in all_elements:
            page_num = page_elements["page_number"]
            print(f"📄 Processando página {page_num} para chunks...")
            
            visual_context = ""
            if page_elements["images"]:
                visual_context += f"\n[PÁGINA {page_num} CONTÉM {len(page_elements['images'])} IMAGENS]"
                for img in page_elements["images"]:
                    if img.get("likely_type") in ["chart", "graph"]:
                        visual_context += f"\n[GRÁFICO: {img['dimensions']}]"
            
            if page_elements["tables"]:
                visual_context += f"\n[PÁGINA {page_num} CONTÉM {len(page_elements['tables'])} TABELAS]"
            
            for block in page_elements["structured_blocks"]:
                block_content = block["content"]
                block_type = block["type"]
                
                content_with_context = block_content
                if visual_context and block_type in ["heading", "paragraph"]:
                    content_with_context = block_content + visual_context
                    visual_context = ""
                
                if (len(current_chunk["content"]) + len(content_with_context) > self.chunk_size and 
                    len(current_chunk["content"]) > self.min_chunk_size):
                    
                    self._finalize_chunk(current_chunk, chunk_counter)
                    chunks.append(current_chunk)
                    
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
                
                if current_chunk["content"]:
                    current_chunk["content"] += "\n\n"
                current_chunk["content"] += content_with_context
                
                if page_num not in current_chunk["metadata"]["pages"]:
                    current_chunk["metadata"]["pages"].append(page_num)
                current_chunk["metadata"]["elements"].append(block["id"])
                if block_type not in current_chunk["metadata"]["content_types"]:
                    current_chunk["metadata"]["content_types"].append(block_type)
        
        if current_chunk["content"].strip():
            self._finalize_chunk(current_chunk, chunk_counter)
            chunks.append(current_chunk)
        
        print(f"✅ Criados {len(chunks)} chunks de conteúdo")
        
        for i, chunk in enumerate(chunks):
            chunk["context"]["chunk_position"] = f"{i + 1}/{len(chunks)}"
            if i > 0:
                chunk["context"]["previous_chunk_id"] = chunks[i - 1]["id"]
            if i < len(chunks) - 1:
                chunk["context"]["next_chunk_id"] = chunks[i + 1]["id"]
        
        return chunks

    def _finalize_chunk(self, chunk: Dict, chunk_id: int):
        """Finaliza um chunk calculando metadados."""
        chunk["id"] = f"chunk_{chunk_id}"
        chunk["metadata"]["word_count"] = len(chunk["content"].split())
        chunk["metadata"]["char_count"] = len(chunk["content"])

    def _get_overlap_content(self, content: str) -> str:
        """Obtém conteúdo de sobreposição do chunk anterior."""
        if len(content) <= self.overlap:
            return content
        
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
        
        summary = ' '.join(words[:10]) + " ... " + ' '.join(words[-10:])
        return summary

    def _get_section_context(self, content: str) -> str:
        """Identifica o contexto da seção atual."""
        content_upper = content.upper()
        
        if any(keyword in content_upper for keyword in ["CONFIDENTIAL", "MEMORANDUM"]):
            return "document_header"
        elif any(keyword in content_upper for keyword in ["RISK", "WARNING", "CAUTION"]):
            return "risk_section"
        elif any(keyword in content_upper for keyword in ["INVESTMENT", "FUND", "PORTFOLIO"]):
            return "investment_section"
        elif any(keyword in content_upper for keyword in ["LEGAL", "REGULATORY", "COMPLIANCE"]):
            return "legal_section"
        elif re.search(r'\d+\.\d+%|\$\d+|USD|EUR', content):
            return "financial_data"
        else:
            return "general_content"
    
    def extract_to_chunks(self, file_path: str, output_dir: str = "C:/extrair", 
                         fund_identifier: str = None, map_id: int = None) -> Dict[str, Any]:
        """Extração principal para chunks contextuais com dados SQL."""
        
        file_path = Path(file_path)
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"EXTRAÇÃO PDF PARA CHUNKS CONTEXTUAIS COM SQL")
        print(f"Arquivo: {file_path.name}")
        print(f"Saída: {output_dir}")
        
        if map_id:
            print(f"MapID: {map_id}")
        elif fund_identifier:
            print(f"Identificador do fundo: {fund_identifier}")
        else:
            print(f"Identificador do fundo: Auto-detectar")
        
        try:
            doc = fitz.open(str(file_path))
            print(f"Total de páginas: {len(doc)}")
            
            # 1. Extrair metadados com integração SQL
            doc_metadata = self.extract_document_metadata(doc, fund_identifier, map_id)
            
            # 2. Extrair elementos de todas as páginas
            all_elements = []
            for page_num in range(len(doc)):
                page_elements = self.extract_page_elements(doc, page_num)
                all_elements.append(page_elements)
            
            # 3. Criar chunks contextuais
            content_chunks = self.create_content_chunks(all_elements)
            
            # 4. Estrutura final dos dados com informações SQL
            extracted_data = {
                "document_info": {
                    "filename": file_path.name,
                    "source_path": str(file_path),
                    "metadata": doc_metadata,
                    "extraction_config": {
                        "chunk_size": self.chunk_size,
                        "overlap": self.overlap,
                        "extraction_timestamp": str(datetime.datetime.now()),
                        "fund_identifier_used": fund_identifier,
                        "map_id_used": map_id
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
                "page_elements": all_elements
            }
            
            # 5. Salvar resultado
            output_file = output_dir / f"{file_path.stem}_chunks.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(extracted_data, f, indent=2, ensure_ascii=False)
            
            # 6. Mostrar resumo com dados SQL
            print(f"\nEXTRAÇÃO CONCLUÍDA!")
            print(f"Estatísticas:")
            print(f" - Chunks criados: {extracted_data['summary']['total_chunks']}")
            print(f" - Palavras totais: {extracted_data['summary']['total_words']:,}")
            print(f" - Imagens detectadas: {extracted_data['summary']['total_images']}")
            print(f" - Tabelas detectadas: {extracted_data['summary']['total_tables']}")
            
            # Mostrar informações SQL se disponível
            sql_info = doc_metadata.get("fund_database_info", {})
            if sql_info.get("sql_data_available", False):
                print(f"\nINFORMAÇÕES DO FUNDO (SQL):")
                print(f" - MapID: {sql_info.get('map_id', 'N/A')}")
                print(f" - Gestor: {sql_info.get('management_company', 'N/A')}")
                print(f" - Fundo: {sql_info.get('fund_name', 'N/A')}")
                print(f" - Nome curto: {sql_info.get('fund_short_name', 'N/A')}")
                print(f" - Asset class: {sql_info.get('asset_class_report', 'N/A')}")
                print(f" - Moeda: {sql_info.get('return_currency', 'N/A')}")
            else:
                print(f"\nInformações SQL não disponíveis")
                if sql_info.get("total_records_found", 0) > 1:
                    print(f"Sugestão: Use MapID específico para seleção precisa")
            
            print(f"\nArquivo salvo: {output_file}")
            
            doc.close()
            return extracted_data
            
        except Exception as e:
            print(f"Erro na extração: {e}")
            return None


def main():
    """Função principal com suporte a MapID e identificador de fundo."""
    print("Extrator PDF para Chunks Contextuais - Com Integração SQL")
    
    if len(sys.argv) < 2:
        print("\nCOMO USAR:")
        print(" python pdf_extractor.py <arquivo.pdf>")
        print(" python pdf_extractor.py <arquivo.pdf> <fund_identifier>")
        print(" python pdf_extractor.py <arquivo.pdf> --map-id <MapID>")
        print(" python pdf_extractor.py <arquivo.pdf> --map-id <MapID> <chunk_size>")
        print("\nExemplos:")
        print(" python pdf_extractor.py documento.pdf")
        print(" python pdf_extractor.py documento.pdf 'Pershing Square'")
        print(" python pdf_extractor.py documento.pdf --map-id 123")
        print(" python pdf_extractor.py documento.pdf --map-id 123 1500")
        return

    pdf_file = sys.argv[1]
    fund_identifier = None
    map_id = None
    chunk_size = 1000
    overlap = 200
    
    # Processar argumentos
    i = 2
    while i < len(sys.argv):
        if sys.argv[i] == "--map-id" and i + 1 < len(sys.argv):
            try:
                map_id = int(sys.argv[i + 1])
                i += 2
            except ValueError:
                print(f"Erro: MapID deve ser um número inteiro: {sys.argv[i + 1]}")
                return
        elif sys.argv[i].isdigit() and not map_id:  # chunk_size
            chunk_size = int(sys.argv[i])
            i += 1
        elif not fund_identifier and not map_id:  # fund_identifier
            fund_identifier = sys.argv[i]
            i += 1
        else:
            i += 1
    
    if not Path(pdf_file).exists():
        print(f"Arquivo não encontrado: {pdf_file}")
        return
    
    print(f"\nParametros:")
    print(f" - Arquivo: {pdf_file}")
    if map_id:
        print(f" - MapID: {map_id}")
    elif fund_identifier:
        print(f" - Identificador: {fund_identifier}")
    else:
        print(f" - Detecção automática ativada")
    print(f" - Chunk size: {chunk_size}")
    
    # Criar extrator e executar
    extractor = PDFToChunksExtractor(chunk_size=chunk_size, overlap=overlap)
    result = extractor.extract_to_chunks(pdf_file, fund_identifier=fund_identifier, map_id=map_id)
    
    if result:
        print(f"\nRESULTADO OTIMIZADO PARA LLM!")
        print(f"{result['summary']['total_chunks']} chunks prontos para análise")
        print(f"Cada chunk mantém contexto + informações do fundo do SQL")


if __name__ == "__main__":
    print("\nEXEMPLO PARA SEU ARQUIVO:")
    print(" python pdf_extractor.py C:\\extrair\\paginas.pdf --map-id 123")
    print(" python pdf_extractor.py C:\\extrair\\paginas.pdf 'Pershing Square'")
    print()
    main(),         # Linhas com uma letra só
        ]
        
        lines = text.split('\n')
        cleaned_lines = []
        
        for line in lines:
            line_clean = line.strip()
            # Pular linhas que são apenas separadores
            if any(re.match(pattern, line_clean, re.IGNORECASE) for pattern in separation_patterns):
                continue
            # Pular linhas muito curtas que são apenas ruído
            if len(line_clean) < 3 and not re.match(r'^\d+
        """Classifica o tipo de bloco baseado no conteúdo e formatação."""
        text_upper = text.upper()
        
        if any(keyword in text_upper for keyword in 
               ["CONFIDENTIAL", "MEMORANDUM", "FUND", "NOTICE", "REGULATORY"]):
            return "heading"
        
        if re.match(r'^\s*[-•▪▫]\s+', text) or re.match(r'^\s*\d+\.\s+', text):
            return "list_item"
        
        if len(re.findall(r'\b\d+\b', text)) > 3 and ('\t' in text or '  ' in text):
            return "table_data"
        
        if len(text) < 200 and (text.startswith('*') or text.startswith('Note:')):
            return "footnote"
        
        if len(text) > 100:
            return "paragraph"
        
        return "text_block"

    def _classify_image_type(self, img_data: Dict) -> str:
        """Classifica o tipo de imagem baseado nas características."""
        width = img_data.get("width", 0)
        height = img_data.get("height", 0)
        pixels = width * height
        aspect_ratio = width / height if height > 0 else 1
        
        if (pixels > 50000 and 
            0.5 <= aspect_ratio <= 3.0 and
            width > 300 and height > 200):
            return "chart"
        
        if pixels > 20000 and aspect_ratio > 2.5:
            return "diagram"
        
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
            
            if (re.search(r'\d+.*\d+.*\d+', line) or 
                '\t' in line or 
                len(re.findall(r'\s{3,}', line)) >= 2):
                
                current_table.append({
                    "line_number": i + 1,
                    "content": line
                })
            else:
                if len(current_table) >= 2:
                    tables.append({
                        "id": f"table_{len(tables) + 1}",
                        "start_line": current_table[0]["line_number"],
                        "end_line": current_table[-1]["line_number"],
                        "rows": current_table,
                        "row_count": len(current_table)
                    })
                current_table = []
        
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
        print(f"📦 Criando chunks de conteúdo...")
        
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
        
        for page_elements in all_elements:
            page_num = page_elements["page_number"]
            print(f"📄 Processando página {page_num} para chunks...")
            
            visual_context = ""
            if page_elements["images"]:
                visual_context += f"\n[PÁGINA {page_num} CONTÉM {len(page_elements['images'])} IMAGENS]"
                for img in page_elements["images"]:
                    if img.get("likely_type") in ["chart", "graph"]:
                        visual_context += f"\n[GRÁFICO: {img['dimensions']}]"
            
            if page_elements["tables"]:
                visual_context += f"\n[PÁGINA {page_num} CONTÉM {len(page_elements['tables'])} TABELAS]"
            
            for block in page_elements["structured_blocks"]:
                block_content = block["content"]
                block_type = block["type"]
                
                content_with_context = block_content
                if visual_context and block_type in ["heading", "paragraph"]:
                    content_with_context = block_content + visual_context
                    visual_context = ""
                
                if (len(current_chunk["content"]) + len(content_with_context) > self.chunk_size and 
                    len(current_chunk["content"]) > self.min_chunk_size):
                    
                    self._finalize_chunk(current_chunk, chunk_counter)
                    chunks.append(current_chunk)
                    
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
                
                if current_chunk["content"]:
                    current_chunk["content"] += "\n\n"
                current_chunk["content"] += content_with_context
                
                if page_num not in current_chunk["metadata"]["pages"]:
                    current_chunk["metadata"]["pages"].append(page_num)
                current_chunk["metadata"]["elements"].append(block["id"])
                if block_type not in current_chunk["metadata"]["content_types"]:
                    current_chunk["metadata"]["content_types"].append(block_type)
        
        if current_chunk["content"].strip():
            self._finalize_chunk(current_chunk, chunk_counter)
            chunks.append(current_chunk)
        
        print(f"✅ Criados {len(chunks)} chunks de conteúdo")
        
        for i, chunk in enumerate(chunks):
            chunk["context"]["chunk_position"] = f"{i + 1}/{len(chunks)}"
            if i > 0:
                chunk["context"]["previous_chunk_id"] = chunks[i - 1]["id"]
            if i < len(chunks) - 1:
                chunk["context"]["next_chunk_id"] = chunks[i + 1]["id"]
        
        return chunks

    def _finalize_chunk(self, chunk: Dict, chunk_id: int):
        """Finaliza um chunk calculando metadados."""
        chunk["id"] = f"chunk_{chunk_id}"
        chunk["metadata"]["word_count"] = len(chunk["content"].split())
        chunk["metadata"]["char_count"] = len(chunk["content"])

    def _get_overlap_content(self, content: str) -> str:
        """Obtém conteúdo de sobreposição do chunk anterior."""
        if len(content) <= self.overlap:
            return content
        
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
        
        summary = ' '.join(words[:10]) + " ... " + ' '.join(words[-10:])
        return summary

    def _get_section_context(self, content: str) -> str:
        """Identifica o contexto da seção atual."""
        content_upper = content.upper()
        
        if any(keyword in content_upper for keyword in ["CONFIDENTIAL", "MEMORANDUM"]):
            return "document_header"
        elif any(keyword in content_upper for keyword in ["RISK", "WARNING", "CAUTION"]):
            return "risk_section"
        elif any(keyword in content_upper for keyword in ["INVESTMENT", "FUND", "PORTFOLIO"]):
            return "investment_section"
        elif any(keyword in content_upper for keyword in ["LEGAL", "REGULATORY", "COMPLIANCE"]):
            return "legal_section"
        elif re.search(r'\d+\.\d+%|\$\d+|USD|EUR', content):
            return "financial_data"
        else:
            return "general_content"
    
    def extract_to_chunks(self, file_path: str, output_dir: str = "C:/extrair", 
                         fund_identifier: str = None, map_id: int = None) -> Dict[str, Any]:
        """Extração principal para chunks contextuais com dados SQL."""
        
        file_path = Path(file_path)
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"EXTRAÇÃO PDF PARA CHUNKS CONTEXTUAIS COM SQL")
        print(f"Arquivo: {file_path.name}")
        print(f"Saída: {output_dir}")
        
        if map_id:
            print(f"MapID: {map_id}")
        elif fund_identifier:
            print(f"Identificador do fundo: {fund_identifier}")
        else:
            print(f"Identificador do fundo: Auto-detectar")
        
        try:
            doc = fitz.open(str(file_path))
            print(f"Total de páginas: {len(doc)}")
            
            # 1. Extrair metadados com integração SQL
            doc_metadata = self.extract_document_metadata(doc, fund_identifier, map_id)
            
            # 2. Extrair elementos de todas as páginas
            all_elements = []
            for page_num in range(len(doc)):
                page_elements = self.extract_page_elements(doc, page_num)
                all_elements.append(page_elements)
            
            # 3. Criar chunks contextuais
            content_chunks = self.create_content_chunks(all_elements)
            
            # 4. Estrutura final dos dados com informações SQL
            extracted_data = {
                "document_info": {
                    "filename": file_path.name,
                    "source_path": str(file_path),
                    "metadata": doc_metadata,
                    "extraction_config": {
                        "chunk_size": self.chunk_size,
                        "overlap": self.overlap,
                        "extraction_timestamp": str(datetime.datetime.now()),
                        "fund_identifier_used": fund_identifier,
                        "map_id_used": map_id
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
                "page_elements": all_elements
            }
            
            # 5. Salvar resultado
            output_file = output_dir / f"{file_path.stem}_chunks.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(extracted_data, f, indent=2, ensure_ascii=False)
            
            # 6. Mostrar resumo com dados SQL
            print(f"\nEXTRAÇÃO CONCLUÍDA!")
            print(f"Estatísticas:")
            print(f" - Chunks criados: {extracted_data['summary']['total_chunks']}")
            print(f" - Palavras totais: {extracted_data['summary']['total_words']:,}")
            print(f" - Imagens detectadas: {extracted_data['summary']['total_images']}")
            print(f" - Tabelas detectadas: {extracted_data['summary']['total_tables']}")
            
            # Mostrar informações SQL se disponível
            sql_info = doc_metadata.get("fund_database_info", {})
            if sql_info.get("sql_data_available", False):
                print(f"\nINFORMAÇÕES DO FUNDO (SQL):")
                print(f" - MapID: {sql_info.get('map_id', 'N/A')}")
                print(f" - Gestor: {sql_info.get('management_company', 'N/A')}")
                print(f" - Fundo: {sql_info.get('fund_name', 'N/A')}")
                print(f" - Nome curto: {sql_info.get('fund_short_name', 'N/A')}")
                print(f" - Asset class: {sql_info.get('asset_class_report', 'N/A')}")
                print(f" - Moeda: {sql_info.get('return_currency', 'N/A')}")
            else:
                print(f"\nInformações SQL não disponíveis")
                if sql_info.get("total_records_found", 0) > 1:
                    print(f"Sugestão: Use MapID específico para seleção precisa")
            
            print(f"\nArquivo salvo: {output_file}")
            
            doc.close()
            return extracted_data
            
        except Exception as e:
            print(f"Erro na extração: {e}")
            return None


def main():
    """Função principal com suporte a MapID e identificador de fundo."""
    print("Extrator PDF para Chunks Contextuais - Com Integração SQL")
    
    if len(sys.argv) < 2:
        print("\nCOMO USAR:")
        print(" python pdf_extractor.py <arquivo.pdf>")
        print(" python pdf_extractor.py <arquivo.pdf> <fund_identifier>")
        print(" python pdf_extractor.py <arquivo.pdf> --map-id <MapID>")
        print(" python pdf_extractor.py <arquivo.pdf> --map-id <MapID> <chunk_size>")
        print("\nExemplos:")
        print(" python pdf_extractor.py documento.pdf")
        print(" python pdf_extractor.py documento.pdf 'Pershing Square'")
        print(" python pdf_extractor.py documento.pdf --map-id 123")
        print(" python pdf_extractor.py documento.pdf --map-id 123 1500")
        return

    pdf_file = sys.argv[1]
    fund_identifier = None
    map_id = None
    chunk_size = 1000
    overlap = 200
    
    # Processar argumentos
    i = 2
    while i < len(sys.argv):
        if sys.argv[i] == "--map-id" and i + 1 < len(sys.argv):
            try:
                map_id = int(sys.argv[i + 1])
                i += 2
            except ValueError:
                print(f"Erro: MapID deve ser um número inteiro: {sys.argv[i + 1]}")
                return
        elif sys.argv[i].isdigit() and not map_id:  # chunk_size
            chunk_size = int(sys.argv[i])
            i += 1
        elif not fund_identifier and not map_id:  # fund_identifier
            fund_identifier = sys.argv[i]
            i += 1
        else:
            i += 1
    
    if not Path(pdf_file).exists():
        print(f"Arquivo não encontrado: {pdf_file}")
        return
    
    print(f"\nParametros:")
    print(f" - Arquivo: {pdf_file}")
    if map_id:
        print(f" - MapID: {map_id}")
    elif fund_identifier:
        print(f" - Identificador: {fund_identifier}")
    else:
        print(f" - Detecção automática ativada")
    print(f" - Chunk size: {chunk_size}")
    
    # Criar extrator e executar
    extractor = PDFToChunksExtractor(chunk_size=chunk_size, overlap=overlap)
    result = extractor.extract_to_chunks(pdf_file, fund_identifier=fund_identifier, map_id=map_id)
    
    if result:
        print(f"\nRESULTADO OTIMIZADO PARA LLM!")
        print(f"{result['summary']['total_chunks']} chunks prontos para análise")
        print(f"Cada chunk mantém contexto + informações do fundo do SQL")


if __name__ == "__main__":
    print("\nEXEMPLO PARA SEU ARQUIVO:")
    print(" python pdf_extractor.py C:\\extrair\\paginas.pdf --map-id 123")
    print(" python pdf_extractor.py C:\\extrair\\paginas.pdf 'Pershing Square'")
    print()
    main(), line_clean):
                continue
            cleaned_lines.append(line)
        
        text = '\n'.join(cleaned_lines)
        
        # 4. Remover espaços extras após limpeza
        text = re.sub(r'\n\s*\n\s*\n', '\n\n', text)  # Múltiplas linhas vazias -> máximo 2
        text = re.sub(r'[ \t]+', ' ', text)           # Múltiplos espaços -> 1 espaço
        
        # 5. Remover caracteres de formatação inúteis
        formatting_patterns = [
            r'\u00a0+',                  # Non-breaking spaces
            r'\u200b+',                  # Zero-width spaces
            r'\u2003+',                  # Em spaces
            r'\u2002+',                  # En spaces
            r'\ufeff',                   # Byte order mark
        ]
        
        for pattern in formatting_patterns:
            text = re.sub(pattern, ' ', text)
        
        # 6. Limpar referências de página desnecessárias
        page_ref_patterns = [
            r'\b\d+\s*\|\s*Page\b',      # "1 | Page"
            r'\bPage\s+\d+\s+of\s+\d+\b', # "Page 1 of 10"
            r'\b\d+\s*/\s*\d+\b',        # "1/10"
        ]
        
        for pattern in page_ref_patterns:
            text = re.sub(pattern, '', text, flags=re.IGNORECASE)
        
        return text.strip()

    @staticmethod
    def clean_block_content(content: str) -> str:
        """Limpeza específica para conteúdo de blocos."""
        if not content or not content.strip():
            return ""
        
        # Aplicar limpeza geral
        content = PDFToChunksExtractor.clean_extracted_text(content)
        
        # Limpezas específicas para blocos
        
        # 1. Remover marcadores de índice desnecessários
        index_patterns = [
            r'\.{2,}\s*\d+\s*
        """Classifica o tipo de bloco baseado no conteúdo e formatação."""
        text_upper = text.upper()
        
        if any(keyword in text_upper for keyword in 
               ["CONFIDENTIAL", "MEMORANDUM", "FUND", "NOTICE", "REGULATORY"]):
            return "heading"
        
        if re.match(r'^\s*[-•▪▫]\s+', text) or re.match(r'^\s*\d+\.\s+', text):
            return "list_item"
        
        if len(re.findall(r'\b\d+\b', text)) > 3 and ('\t' in text or '  ' in text):
            return "table_data"
        
        if len(text) < 200 and (text.startswith('*') or text.startswith('Note:')):
            return "footnote"
        
        if len(text) > 100:
            return "paragraph"
        
        return "text_block"

    def _classify_image_type(self, img_data: Dict) -> str:
        """Classifica o tipo de imagem baseado nas características."""
        width = img_data.get("width", 0)
        height = img_data.get("height", 0)
        pixels = width * height
        aspect_ratio = width / height if height > 0 else 1
        
        if (pixels > 50000 and 
            0.5 <= aspect_ratio <= 3.0 and
            width > 300 and height > 200):
            return "chart"
        
        if pixels > 20000 and aspect_ratio > 2.5:
            return "diagram"
        
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
            
            if (re.search(r'\d+.*\d+.*\d+', line) or 
                '\t' in line or 
                len(re.findall(r'\s{3,}', line)) >= 2):
                
                current_table.append({
                    "line_number": i + 1,
                    "content": line
                })
            else:
                if len(current_table) >= 2:
                    tables.append({
                        "id": f"table_{len(tables) + 1}",
                        "start_line": current_table[0]["line_number"],
                        "end_line": current_table[-1]["line_number"],
                        "rows": current_table,
                        "row_count": len(current_table)
                    })
                current_table = []
        
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
        print(f"📦 Criando chunks de conteúdo...")
        
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
        
        for page_elements in all_elements:
            page_num = page_elements["page_number"]
            print(f"📄 Processando página {page_num} para chunks...")
            
            visual_context = ""
            if page_elements["images"]:
                visual_context += f"\n[PÁGINA {page_num} CONTÉM {len(page_elements['images'])} IMAGENS]"
                for img in page_elements["images"]:
                    if img.get("likely_type") in ["chart", "graph"]:
                        visual_context += f"\n[GRÁFICO: {img['dimensions']}]"
            
            if page_elements["tables"]:
                visual_context += f"\n[PÁGINA {page_num} CONTÉM {len(page_elements['tables'])} TABELAS]"
            
            for block in page_elements["structured_blocks"]:
                block_content = block["content"]
                block_type = block["type"]
                
                content_with_context = block_content
                if visual_context and block_type in ["heading", "paragraph"]:
                    content_with_context = block_content + visual_context
                    visual_context = ""
                
                if (len(current_chunk["content"]) + len(content_with_context) > self.chunk_size and 
                    len(current_chunk["content"]) > self.min_chunk_size):
                    
                    self._finalize_chunk(current_chunk, chunk_counter)
                    chunks.append(current_chunk)
                    
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
                
                if current_chunk["content"]:
                    current_chunk["content"] += "\n\n"
                current_chunk["content"] += content_with_context
                
                if page_num not in current_chunk["metadata"]["pages"]:
                    current_chunk["metadata"]["pages"].append(page_num)
                current_chunk["metadata"]["elements"].append(block["id"])
                if block_type not in current_chunk["metadata"]["content_types"]:
                    current_chunk["metadata"]["content_types"].append(block_type)
        
        if current_chunk["content"].strip():
            self._finalize_chunk(current_chunk, chunk_counter)
            chunks.append(current_chunk)
        
        print(f"✅ Criados {len(chunks)} chunks de conteúdo")
        
        for i, chunk in enumerate(chunks):
            chunk["context"]["chunk_position"] = f"{i + 1}/{len(chunks)}"
            if i > 0:
                chunk["context"]["previous_chunk_id"] = chunks[i - 1]["id"]
            if i < len(chunks) - 1:
                chunk["context"]["next_chunk_id"] = chunks[i + 1]["id"]
        
        return chunks

    def _finalize_chunk(self, chunk: Dict, chunk_id: int):
        """Finaliza um chunk calculando metadados."""
        chunk["id"] = f"chunk_{chunk_id}"
        chunk["metadata"]["word_count"] = len(chunk["content"].split())
        chunk["metadata"]["char_count"] = len(chunk["content"])

    def _get_overlap_content(self, content: str) -> str:
        """Obtém conteúdo de sobreposição do chunk anterior."""
        if len(content) <= self.overlap:
            return content
        
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
        
        summary = ' '.join(words[:10]) + " ... " + ' '.join(words[-10:])
        return summary

    def _get_section_context(self, content: str) -> str:
        """Identifica o contexto da seção atual."""
        content_upper = content.upper()
        
        if any(keyword in content_upper for keyword in ["CONFIDENTIAL", "MEMORANDUM"]):
            return "document_header"
        elif any(keyword in content_upper for keyword in ["RISK", "WARNING", "CAUTION"]):
            return "risk_section"
        elif any(keyword in content_upper for keyword in ["INVESTMENT", "FUND", "PORTFOLIO"]):
            return "investment_section"
        elif any(keyword in content_upper for keyword in ["LEGAL", "REGULATORY", "COMPLIANCE"]):
            return "legal_section"
        elif re.search(r'\d+\.\d+%|\$\d+|USD|EUR', content):
            return "financial_data"
        else:
            return "general_content"
    
    def extract_to_chunks(self, file_path: str, output_dir: str = "C:/extrair", 
                         fund_identifier: str = None, map_id: int = None) -> Dict[str, Any]:
        """Extração principal para chunks contextuais com dados SQL."""
        
        file_path = Path(file_path)
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"EXTRAÇÃO PDF PARA CHUNKS CONTEXTUAIS COM SQL")
        print(f"Arquivo: {file_path.name}")
        print(f"Saída: {output_dir}")
        
        if map_id:
            print(f"MapID: {map_id}")
        elif fund_identifier:
            print(f"Identificador do fundo: {fund_identifier}")
        else:
            print(f"Identificador do fundo: Auto-detectar")
        
        try:
            doc = fitz.open(str(file_path))
            print(f"Total de páginas: {len(doc)}")
            
            # 1. Extrair metadados com integração SQL
            doc_metadata = self.extract_document_metadata(doc, fund_identifier, map_id)
            
            # 2. Extrair elementos de todas as páginas
            all_elements = []
            for page_num in range(len(doc)):
                page_elements = self.extract_page_elements(doc, page_num)
                all_elements.append(page_elements)
            
            # 3. Criar chunks contextuais
            content_chunks = self.create_content_chunks(all_elements)
            
            # 4. Estrutura final dos dados com informações SQL
            extracted_data = {
                "document_info": {
                    "filename": file_path.name,
                    "source_path": str(file_path),
                    "metadata": doc_metadata,
                    "extraction_config": {
                        "chunk_size": self.chunk_size,
                        "overlap": self.overlap,
                        "extraction_timestamp": str(datetime.datetime.now()),
                        "fund_identifier_used": fund_identifier,
                        "map_id_used": map_id
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
                "page_elements": all_elements
            }
            
            # 5. Salvar resultado
            output_file = output_dir / f"{file_path.stem}_chunks.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(extracted_data, f, indent=2, ensure_ascii=False)
            
            # 6. Mostrar resumo com dados SQL
            print(f"\nEXTRAÇÃO CONCLUÍDA!")
            print(f"Estatísticas:")
            print(f" - Chunks criados: {extracted_data['summary']['total_chunks']}")
            print(f" - Palavras totais: {extracted_data['summary']['total_words']:,}")
            print(f" - Imagens detectadas: {extracted_data['summary']['total_images']}")
            print(f" - Tabelas detectadas: {extracted_data['summary']['total_tables']}")
            
            # Mostrar informações SQL se disponível
            sql_info = doc_metadata.get("fund_database_info", {})
            if sql_info.get("sql_data_available", False):
                print(f"\nINFORMAÇÕES DO FUNDO (SQL):")
                print(f" - MapID: {sql_info.get('map_id', 'N/A')}")
                print(f" - Gestor: {sql_info.get('management_company', 'N/A')}")
                print(f" - Fundo: {sql_info.get('fund_name', 'N/A')}")
                print(f" - Nome curto: {sql_info.get('fund_short_name', 'N/A')}")
                print(f" - Asset class: {sql_info.get('asset_class_report', 'N/A')}")
                print(f" - Moeda: {sql_info.get('return_currency', 'N/A')}")
            else:
                print(f"\nInformações SQL não disponíveis")
                if sql_info.get("total_records_found", 0) > 1:
                    print(f"Sugestão: Use MapID específico para seleção precisa")
            
            print(f"\nArquivo salvo: {output_file}")
            
            doc.close()
            return extracted_data
            
        except Exception as e:
            print(f"Erro na extração: {e}")
            return None


def main():
    """Função principal com suporte a MapID e identificador de fundo."""
    print("Extrator PDF para Chunks Contextuais - Com Integração SQL")
    
    if len(sys.argv) < 2:
        print("\nCOMO USAR:")
        print(" python pdf_extractor.py <arquivo.pdf>")
        print(" python pdf_extractor.py <arquivo.pdf> <fund_identifier>")
        print(" python pdf_extractor.py <arquivo.pdf> --map-id <MapID>")
        print(" python pdf_extractor.py <arquivo.pdf> --map-id <MapID> <chunk_size>")
        print("\nExemplos:")
        print(" python pdf_extractor.py documento.pdf")
        print(" python pdf_extractor.py documento.pdf 'Pershing Square'")
        print(" python pdf_extractor.py documento.pdf --map-id 123")
        print(" python pdf_extractor.py documento.pdf --map-id 123 1500")
        return

    pdf_file = sys.argv[1]
    fund_identifier = None
    map_id = None
    chunk_size = 1000
    overlap = 200
    
    # Processar argumentos
    i = 2
    while i < len(sys.argv):
        if sys.argv[i] == "--map-id" and i + 1 < len(sys.argv):
            try:
                map_id = int(sys.argv[i + 1])
                i += 2
            except ValueError:
                print(f"Erro: MapID deve ser um número inteiro: {sys.argv[i + 1]}")
                return
        elif sys.argv[i].isdigit() and not map_id:  # chunk_size
            chunk_size = int(sys.argv[i])
            i += 1
        elif not fund_identifier and not map_id:  # fund_identifier
            fund_identifier = sys.argv[i]
            i += 1
        else:
            i += 1
    
    if not Path(pdf_file).exists():
        print(f"Arquivo não encontrado: {pdf_file}")
        return
    
    print(f"\nParametros:")
    print(f" - Arquivo: {pdf_file}")
    if map_id:
        print(f" - MapID: {map_id}")
    elif fund_identifier:
        print(f" - Identificador: {fund_identifier}")
    else:
        print(f" - Detecção automática ativada")
    print(f" - Chunk size: {chunk_size}")
    
    # Criar extrator e executar
    extractor = PDFToChunksExtractor(chunk_size=chunk_size, overlap=overlap)
    result = extractor.extract_to_chunks(pdf_file, fund_identifier=fund_identifier, map_id=map_id)
    
    if result:
        print(f"\nRESULTADO OTIMIZADO PARA LLM!")
        print(f"{result['summary']['total_chunks']} chunks prontos para análise")
        print(f"Cada chunk mantém contexto + informações do fundo do SQL")


if __name__ == "__main__":
    print("\nEXEMPLO PARA SEU ARQUIVO:")
    print(" python pdf_extractor.py C:\\extrair\\paginas.pdf --map-id 123")
    print(" python pdf_extractor.py C:\\extrair\\paginas.pdf 'Pershing Square'")
    print()
    main(),         # "texto........... 25"
            r'\s+\d+\s*
        """Classifica o tipo de bloco baseado no conteúdo e formatação."""
        text_upper = text.upper()
        
        if any(keyword in text_upper for keyword in 
               ["CONFIDENTIAL", "MEMORANDUM", "FUND", "NOTICE", "REGULATORY"]):
            return "heading"
        
        if re.match(r'^\s*[-•▪▫]\s+', text) or re.match(r'^\s*\d+\.\s+', text):
            return "list_item"
        
        if len(re.findall(r'\b\d+\b', text)) > 3 and ('\t' in text or '  ' in text):
            return "table_data"
        
        if len(text) < 200 and (text.startswith('*') or text.startswith('Note:')):
            return "footnote"
        
        if len(text) > 100:
            return "paragraph"
        
        return "text_block"

    def _classify_image_type(self, img_data: Dict) -> str:
        """Classifica o tipo de imagem baseado nas características."""
        width = img_data.get("width", 0)
        height = img_data.get("height", 0)
        pixels = width * height
        aspect_ratio = width / height if height > 0 else 1
        
        if (pixels > 50000 and 
            0.5 <= aspect_ratio <= 3.0 and
            width > 300 and height > 200):
            return "chart"
        
        if pixels > 20000 and aspect_ratio > 2.5:
            return "diagram"
        
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
            
            if (re.search(r'\d+.*\d+.*\d+', line) or 
                '\t' in line or 
                len(re.findall(r'\s{3,}', line)) >= 2):
                
                current_table.append({
                    "line_number": i + 1,
                    "content": line
                })
            else:
                if len(current_table) >= 2:
                    tables.append({
                        "id": f"table_{len(tables) + 1}",
                        "start_line": current_table[0]["line_number"],
                        "end_line": current_table[-1]["line_number"],
                        "rows": current_table,
                        "row_count": len(current_table)
                    })
                current_table = []
        
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
        print(f"📦 Criando chunks de conteúdo...")
        
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
        
        for page_elements in all_elements:
            page_num = page_elements["page_number"]
            print(f"📄 Processando página {page_num} para chunks...")
            
            visual_context = ""
            if page_elements["images"]:
                visual_context += f"\n[PÁGINA {page_num} CONTÉM {len(page_elements['images'])} IMAGENS]"
                for img in page_elements["images"]:
                    if img.get("likely_type") in ["chart", "graph"]:
                        visual_context += f"\n[GRÁFICO: {img['dimensions']}]"
            
            if page_elements["tables"]:
                visual_context += f"\n[PÁGINA {page_num} CONTÉM {len(page_elements['tables'])} TABELAS]"
            
            for block in page_elements["structured_blocks"]:
                block_content = block["content"]
                block_type = block["type"]
                
                content_with_context = block_content
                if visual_context and block_type in ["heading", "paragraph"]:
                    content_with_context = block_content + visual_context
                    visual_context = ""
                
                if (len(current_chunk["content"]) + len(content_with_context) > self.chunk_size and 
                    len(current_chunk["content"]) > self.min_chunk_size):
                    
                    self._finalize_chunk(current_chunk, chunk_counter)
                    chunks.append(current_chunk)
                    
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
                
                if current_chunk["content"]:
                    current_chunk["content"] += "\n\n"
                current_chunk["content"] += content_with_context
                
                if page_num not in current_chunk["metadata"]["pages"]:
                    current_chunk["metadata"]["pages"].append(page_num)
                current_chunk["metadata"]["elements"].append(block["id"])
                if block_type not in current_chunk["metadata"]["content_types"]:
                    current_chunk["metadata"]["content_types"].append(block_type)
        
        if current_chunk["content"].strip():
            self._finalize_chunk(current_chunk, chunk_counter)
            chunks.append(current_chunk)
        
        print(f"✅ Criados {len(chunks)} chunks de conteúdo")
        
        for i, chunk in enumerate(chunks):
            chunk["context"]["chunk_position"] = f"{i + 1}/{len(chunks)}"
            if i > 0:
                chunk["context"]["previous_chunk_id"] = chunks[i - 1]["id"]
            if i < len(chunks) - 1:
                chunk["context"]["next_chunk_id"] = chunks[i + 1]["id"]
        
        return chunks

    def _finalize_chunk(self, chunk: Dict, chunk_id: int):
        """Finaliza um chunk calculando metadados."""
        chunk["id"] = f"chunk_{chunk_id}"
        chunk["metadata"]["word_count"] = len(chunk["content"].split())
        chunk["metadata"]["char_count"] = len(chunk["content"])

    def _get_overlap_content(self, content: str) -> str:
        """Obtém conteúdo de sobreposição do chunk anterior."""
        if len(content) <= self.overlap:
            return content
        
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
        
        summary = ' '.join(words[:10]) + " ... " + ' '.join(words[-10:])
        return summary

    def _get_section_context(self, content: str) -> str:
        """Identifica o contexto da seção atual."""
        content_upper = content.upper()
        
        if any(keyword in content_upper for keyword in ["CONFIDENTIAL", "MEMORANDUM"]):
            return "document_header"
        elif any(keyword in content_upper for keyword in ["RISK", "WARNING", "CAUTION"]):
            return "risk_section"
        elif any(keyword in content_upper for keyword in ["INVESTMENT", "FUND", "PORTFOLIO"]):
            return "investment_section"
        elif any(keyword in content_upper for keyword in ["LEGAL", "REGULATORY", "COMPLIANCE"]):
            return "legal_section"
        elif re.search(r'\d+\.\d+%|\$\d+|USD|EUR', content):
            return "financial_data"
        else:
            return "general_content"
    
    def extract_to_chunks(self, file_path: str, output_dir: str = "C:/extrair", 
                         fund_identifier: str = None, map_id: int = None) -> Dict[str, Any]:
        """Extração principal para chunks contextuais com dados SQL."""
        
        file_path = Path(file_path)
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"EXTRAÇÃO PDF PARA CHUNKS CONTEXTUAIS COM SQL")
        print(f"Arquivo: {file_path.name}")
        print(f"Saída: {output_dir}")
        
        if map_id:
            print(f"MapID: {map_id}")
        elif fund_identifier:
            print(f"Identificador do fundo: {fund_identifier}")
        else:
            print(f"Identificador do fundo: Auto-detectar")
        
        try:
            doc = fitz.open(str(file_path))
            print(f"Total de páginas: {len(doc)}")
            
            # 1. Extrair metadados com integração SQL
            doc_metadata = self.extract_document_metadata(doc, fund_identifier, map_id)
            
            # 2. Extrair elementos de todas as páginas
            all_elements = []
            for page_num in range(len(doc)):
                page_elements = self.extract_page_elements(doc, page_num)
                all_elements.append(page_elements)
            
            # 3. Criar chunks contextuais
            content_chunks = self.create_content_chunks(all_elements)
            
            # 4. Estrutura final dos dados com informações SQL
            extracted_data = {
                "document_info": {
                    "filename": file_path.name,
                    "source_path": str(file_path),
                    "metadata": doc_metadata,
                    "extraction_config": {
                        "chunk_size": self.chunk_size,
                        "overlap": self.overlap,
                        "extraction_timestamp": str(datetime.datetime.now()),
                        "fund_identifier_used": fund_identifier,
                        "map_id_used": map_id
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
                "page_elements": all_elements
            }
            
            # 5. Salvar resultado
            output_file = output_dir / f"{file_path.stem}_chunks.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(extracted_data, f, indent=2, ensure_ascii=False)
            
            # 6. Mostrar resumo com dados SQL
            print(f"\nEXTRAÇÃO CONCLUÍDA!")
            print(f"Estatísticas:")
            print(f" - Chunks criados: {extracted_data['summary']['total_chunks']}")
            print(f" - Palavras totais: {extracted_data['summary']['total_words']:,}")
            print(f" - Imagens detectadas: {extracted_data['summary']['total_images']}")
            print(f" - Tabelas detectadas: {extracted_data['summary']['total_tables']}")
            
            # Mostrar informações SQL se disponível
            sql_info = doc_metadata.get("fund_database_info", {})
            if sql_info.get("sql_data_available", False):
                print(f"\nINFORMAÇÕES DO FUNDO (SQL):")
                print(f" - MapID: {sql_info.get('map_id', 'N/A')}")
                print(f" - Gestor: {sql_info.get('management_company', 'N/A')}")
                print(f" - Fundo: {sql_info.get('fund_name', 'N/A')}")
                print(f" - Nome curto: {sql_info.get('fund_short_name', 'N/A')}")
                print(f" - Asset class: {sql_info.get('asset_class_report', 'N/A')}")
                print(f" - Moeda: {sql_info.get('return_currency', 'N/A')}")
            else:
                print(f"\nInformações SQL não disponíveis")
                if sql_info.get("total_records_found", 0) > 1:
                    print(f"Sugestão: Use MapID específico para seleção precisa")
            
            print(f"\nArquivo salvo: {output_file}")
            
            doc.close()
            return extracted_data
            
        except Exception as e:
            print(f"Erro na extração: {e}")
            return None


def main():
    """Função principal com suporte a MapID e identificador de fundo."""
    print("Extrator PDF para Chunks Contextuais - Com Integração SQL")
    
    if len(sys.argv) < 2:
        print("\nCOMO USAR:")
        print(" python pdf_extractor.py <arquivo.pdf>")
        print(" python pdf_extractor.py <arquivo.pdf> <fund_identifier>")
        print(" python pdf_extractor.py <arquivo.pdf> --map-id <MapID>")
        print(" python pdf_extractor.py <arquivo.pdf> --map-id <MapID> <chunk_size>")
        print("\nExemplos:")
        print(" python pdf_extractor.py documento.pdf")
        print(" python pdf_extractor.py documento.pdf 'Pershing Square'")
        print(" python pdf_extractor.py documento.pdf --map-id 123")
        print(" python pdf_extractor.py documento.pdf --map-id 123 1500")
        return

    pdf_file = sys.argv[1]
    fund_identifier = None
    map_id = None
    chunk_size = 1000
    overlap = 200
    
    # Processar argumentos
    i = 2
    while i < len(sys.argv):
        if sys.argv[i] == "--map-id" and i + 1 < len(sys.argv):
            try:
                map_id = int(sys.argv[i + 1])
                i += 2
            except ValueError:
                print(f"Erro: MapID deve ser um número inteiro: {sys.argv[i + 1]}")
                return
        elif sys.argv[i].isdigit() and not map_id:  # chunk_size
            chunk_size = int(sys.argv[i])
            i += 1
        elif not fund_identifier and not map_id:  # fund_identifier
            fund_identifier = sys.argv[i]
            i += 1
        else:
            i += 1
    
    if not Path(pdf_file).exists():
        print(f"Arquivo não encontrado: {pdf_file}")
        return
    
    print(f"\nParametros:")
    print(f" - Arquivo: {pdf_file}")
    if map_id:
        print(f" - MapID: {map_id}")
    elif fund_identifier:
        print(f" - Identificador: {fund_identifier}")
    else:
        print(f" - Detecção automática ativada")
    print(f" - Chunk size: {chunk_size}")
    
    # Criar extrator e executar
    extractor = PDFToChunksExtractor(chunk_size=chunk_size, overlap=overlap)
    result = extractor.extract_to_chunks(pdf_file, fund_identifier=fund_identifier, map_id=map_id)
    
    if result:
        print(f"\nRESULTADO OTIMIZADO PARA LLM!")
        print(f"{result['summary']['total_chunks']} chunks prontos para análise")
        print(f"Cada chunk mantém contexto + informações do fundo do SQL")


if __name__ == "__main__":
    print("\nEXEMPLO PARA SEU ARQUIVO:")
    print(" python pdf_extractor.py C:\\extrair\\paginas.pdf --map-id 123")
    print(" python pdf_extractor.py C:\\extrair\\paginas.pdf 'Pershing Square'")
    print()
    main(),               # "texto    25" (números no final)
        ]
        
        for pattern in index_patterns:
            if re.search(pattern, content):
                content = re.sub(pattern, '', content).strip()
        
        # 2. Limpar cabeçalhos redundantes
        if len(content) < 100:  # Só para textos curtos (possíveis cabeçalhos)
            # Remover repetições do nome da empresa
            content = re.sub(r'\b(Holdings?|Ltd\.?|Inc\.?|Corp\.?|Limited|Company)\s+\1\b', r'\1', content, flags=re.IGNORECASE)
        
        # 3. Normalizar espaçamento final
        content = re.sub(r'\s+', ' ', content).strip()
        
        return content

    @staticmethod
    def should_skip_block(content: str) -> bool:
        """Determina se um bloco deve ser ignorado por ser inútil."""
        if not content or len(content.strip()) < 3:
            return True
        
        content_clean = content.strip().lower()
        
        # Padrões de conteúdo inútil
        useless_patterns = [
            r'^[-_=*+~#\s]*
        """Classifica o tipo de bloco baseado no conteúdo e formatação."""
        text_upper = text.upper()
        
        if any(keyword in text_upper for keyword in 
               ["CONFIDENTIAL", "MEMORANDUM", "FUND", "NOTICE", "REGULATORY"]):
            return "heading"
        
        if re.match(r'^\s*[-•▪▫]\s+', text) or re.match(r'^\s*\d+\.\s+', text):
            return "list_item"
        
        if len(re.findall(r'\b\d+\b', text)) > 3 and ('\t' in text or '  ' in text):
            return "table_data"
        
        if len(text) < 200 and (text.startswith('*') or text.startswith('Note:')):
            return "footnote"
        
        if len(text) > 100:
            return "paragraph"
        
        return "text_block"

    def _classify_image_type(self, img_data: Dict) -> str:
        """Classifica o tipo de imagem baseado nas características."""
        width = img_data.get("width", 0)
        height = img_data.get("height", 0)
        pixels = width * height
        aspect_ratio = width / height if height > 0 else 1
        
        if (pixels > 50000 and 
            0.5 <= aspect_ratio <= 3.0 and
            width > 300 and height > 200):
            return "chart"
        
        if pixels > 20000 and aspect_ratio > 2.5:
            return "diagram"
        
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
            
            if (re.search(r'\d+.*\d+.*\d+', line) or 
                '\t' in line or 
                len(re.findall(r'\s{3,}', line)) >= 2):
                
                current_table.append({
                    "line_number": i + 1,
                    "content": line
                })
            else:
                if len(current_table) >= 2:
                    tables.append({
                        "id": f"table_{len(tables) + 1}",
                        "start_line": current_table[0]["line_number"],
                        "end_line": current_table[-1]["line_number"],
                        "rows": current_table,
                        "row_count": len(current_table)
                    })
                current_table = []
        
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
        print(f"📦 Criando chunks de conteúdo...")
        
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
        
        for page_elements in all_elements:
            page_num = page_elements["page_number"]
            print(f"📄 Processando página {page_num} para chunks...")
            
            visual_context = ""
            if page_elements["images"]:
                visual_context += f"\n[PÁGINA {page_num} CONTÉM {len(page_elements['images'])} IMAGENS]"
                for img in page_elements["images"]:
                    if img.get("likely_type") in ["chart", "graph"]:
                        visual_context += f"\n[GRÁFICO: {img['dimensions']}]"
            
            if page_elements["tables"]:
                visual_context += f"\n[PÁGINA {page_num} CONTÉM {len(page_elements['tables'])} TABELAS]"
            
            for block in page_elements["structured_blocks"]:
                block_content = block["content"]
                block_type = block["type"]
                
                content_with_context = block_content
                if visual_context and block_type in ["heading", "paragraph"]:
                    content_with_context = block_content + visual_context
                    visual_context = ""
                
                if (len(current_chunk["content"]) + len(content_with_context) > self.chunk_size and 
                    len(current_chunk["content"]) > self.min_chunk_size):
                    
                    self._finalize_chunk(current_chunk, chunk_counter)
                    chunks.append(current_chunk)
                    
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
                
                if current_chunk["content"]:
                    current_chunk["content"] += "\n\n"
                current_chunk["content"] += content_with_context
                
                if page_num not in current_chunk["metadata"]["pages"]:
                    current_chunk["metadata"]["pages"].append(page_num)
                current_chunk["metadata"]["elements"].append(block["id"])
                if block_type not in current_chunk["metadata"]["content_types"]:
                    current_chunk["metadata"]["content_types"].append(block_type)
        
        if current_chunk["content"].strip():
            self._finalize_chunk(current_chunk, chunk_counter)
            chunks.append(current_chunk)
        
        print(f"✅ Criados {len(chunks)} chunks de conteúdo")
        
        for i, chunk in enumerate(chunks):
            chunk["context"]["chunk_position"] = f"{i + 1}/{len(chunks)}"
            if i > 0:
                chunk["context"]["previous_chunk_id"] = chunks[i - 1]["id"]
            if i < len(chunks) - 1:
                chunk["context"]["next_chunk_id"] = chunks[i + 1]["id"]
        
        return chunks

    def _finalize_chunk(self, chunk: Dict, chunk_id: int):
        """Finaliza um chunk calculando metadados."""
        chunk["id"] = f"chunk_{chunk_id}"
        chunk["metadata"]["word_count"] = len(chunk["content"].split())
        chunk["metadata"]["char_count"] = len(chunk["content"])

    def _get_overlap_content(self, content: str) -> str:
        """Obtém conteúdo de sobreposição do chunk anterior."""
        if len(content) <= self.overlap:
            return content
        
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
        
        summary = ' '.join(words[:10]) + " ... " + ' '.join(words[-10:])
        return summary

    def _get_section_context(self, content: str) -> str:
        """Identifica o contexto da seção atual."""
        content_upper = content.upper()
        
        if any(keyword in content_upper for keyword in ["CONFIDENTIAL", "MEMORANDUM"]):
            return "document_header"
        elif any(keyword in content_upper for keyword in ["RISK", "WARNING", "CAUTION"]):
            return "risk_section"
        elif any(keyword in content_upper for keyword in ["INVESTMENT", "FUND", "PORTFOLIO"]):
            return "investment_section"
        elif any(keyword in content_upper for keyword in ["LEGAL", "REGULATORY", "COMPLIANCE"]):
            return "legal_section"
        elif re.search(r'\d+\.\d+%|\$\d+|USD|EUR', content):
            return "financial_data"
        else:
            return "general_content"
    
    def extract_to_chunks(self, file_path: str, output_dir: str = "C:/extrair", 
                         fund_identifier: str = None, map_id: int = None) -> Dict[str, Any]:
        """Extração principal para chunks contextuais com dados SQL."""
        
        file_path = Path(file_path)
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"EXTRAÇÃO PDF PARA CHUNKS CONTEXTUAIS COM SQL")
        print(f"Arquivo: {file_path.name}")
        print(f"Saída: {output_dir}")
        
        if map_id:
            print(f"MapID: {map_id}")
        elif fund_identifier:
            print(f"Identificador do fundo: {fund_identifier}")
        else:
            print(f"Identificador do fundo: Auto-detectar")
        
        try:
            doc = fitz.open(str(file_path))
            print(f"Total de páginas: {len(doc)}")
            
            # 1. Extrair metadados com integração SQL
            doc_metadata = self.extract_document_metadata(doc, fund_identifier, map_id)
            
            # 2. Extrair elementos de todas as páginas
            all_elements = []
            for page_num in range(len(doc)):
                page_elements = self.extract_page_elements(doc, page_num)
                all_elements.append(page_elements)
            
            # 3. Criar chunks contextuais
            content_chunks = self.create_content_chunks(all_elements)
            
            # 4. Estrutura final dos dados com informações SQL
            extracted_data = {
                "document_info": {
                    "filename": file_path.name,
                    "source_path": str(file_path),
                    "metadata": doc_metadata,
                    "extraction_config": {
                        "chunk_size": self.chunk_size,
                        "overlap": self.overlap,
                        "extraction_timestamp": str(datetime.datetime.now()),
                        "fund_identifier_used": fund_identifier,
                        "map_id_used": map_id
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
                "page_elements": all_elements
            }
            
            # 5. Salvar resultado
            output_file = output_dir / f"{file_path.stem}_chunks.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(extracted_data, f, indent=2, ensure_ascii=False)
            
            # 6. Mostrar resumo com dados SQL
            print(f"\nEXTRAÇÃO CONCLUÍDA!")
            print(f"Estatísticas:")
            print(f" - Chunks criados: {extracted_data['summary']['total_chunks']}")
            print(f" - Palavras totais: {extracted_data['summary']['total_words']:,}")
            print(f" - Imagens detectadas: {extracted_data['summary']['total_images']}")
            print(f" - Tabelas detectadas: {extracted_data['summary']['total_tables']}")
            
            # Mostrar informações SQL se disponível
            sql_info = doc_metadata.get("fund_database_info", {})
            if sql_info.get("sql_data_available", False):
                print(f"\nINFORMAÇÕES DO FUNDO (SQL):")
                print(f" - MapID: {sql_info.get('map_id', 'N/A')}")
                print(f" - Gestor: {sql_info.get('management_company', 'N/A')}")
                print(f" - Fundo: {sql_info.get('fund_name', 'N/A')}")
                print(f" - Nome curto: {sql_info.get('fund_short_name', 'N/A')}")
                print(f" - Asset class: {sql_info.get('asset_class_report', 'N/A')}")
                print(f" - Moeda: {sql_info.get('return_currency', 'N/A')}")
            else:
                print(f"\nInformações SQL não disponíveis")
                if sql_info.get("total_records_found", 0) > 1:
                    print(f"Sugestão: Use MapID específico para seleção precisa")
            
            print(f"\nArquivo salvo: {output_file}")
            
            doc.close()
            return extracted_data
            
        except Exception as e:
            print(f"Erro na extração: {e}")
            return None


def main():
    """Função principal com suporte a MapID e identificador de fundo."""
    print("Extrator PDF para Chunks Contextuais - Com Integração SQL")
    
    if len(sys.argv) < 2:
        print("\nCOMO USAR:")
        print(" python pdf_extractor.py <arquivo.pdf>")
        print(" python pdf_extractor.py <arquivo.pdf> <fund_identifier>")
        print(" python pdf_extractor.py <arquivo.pdf> --map-id <MapID>")
        print(" python pdf_extractor.py <arquivo.pdf> --map-id <MapID> <chunk_size>")
        print("\nExemplos:")
        print(" python pdf_extractor.py documento.pdf")
        print(" python pdf_extractor.py documento.pdf 'Pershing Square'")
        print(" python pdf_extractor.py documento.pdf --map-id 123")
        print(" python pdf_extractor.py documento.pdf --map-id 123 1500")
        return

    pdf_file = sys.argv[1]
    fund_identifier = None
    map_id = None
    chunk_size = 1000
    overlap = 200
    
    # Processar argumentos
    i = 2
    while i < len(sys.argv):
        if sys.argv[i] == "--map-id" and i + 1 < len(sys.argv):
            try:
                map_id = int(sys.argv[i + 1])
                i += 2
            except ValueError:
                print(f"Erro: MapID deve ser um número inteiro: {sys.argv[i + 1]}")
                return
        elif sys.argv[i].isdigit() and not map_id:  # chunk_size
            chunk_size = int(sys.argv[i])
            i += 1
        elif not fund_identifier and not map_id:  # fund_identifier
            fund_identifier = sys.argv[i]
            i += 1
        else:
            i += 1
    
    if not Path(pdf_file).exists():
        print(f"Arquivo não encontrado: {pdf_file}")
        return
    
    print(f"\nParametros:")
    print(f" - Arquivo: {pdf_file}")
    if map_id:
        print(f" - MapID: {map_id}")
    elif fund_identifier:
        print(f" - Identificador: {fund_identifier}")
    else:
        print(f" - Detecção automática ativada")
    print(f" - Chunk size: {chunk_size}")
    
    # Criar extrator e executar
    extractor = PDFToChunksExtractor(chunk_size=chunk_size, overlap=overlap)
    result = extractor.extract_to_chunks(pdf_file, fund_identifier=fund_identifier, map_id=map_id)
    
    if result:
        print(f"\nRESULTADO OTIMIZADO PARA LLM!")
        print(f"{result['summary']['total_chunks']} chunks prontos para análise")
        print(f"Cada chunk mantém contexto + informações do fundo do SQL")


if __name__ == "__main__":
    print("\nEXEMPLO PARA SEU ARQUIVO:")
    print(" python pdf_extractor.py C:\\extrair\\paginas.pdf --map-id 123")
    print(" python pdf_extractor.py C:\\extrair\\paginas.pdf 'Pershing Square'")
    print()
    main(),           # Só caracteres de separação
            r'^\d+\s*
        """Classifica o tipo de bloco baseado no conteúdo e formatação."""
        text_upper = text.upper()
        
        if any(keyword in text_upper for keyword in 
               ["CONFIDENTIAL", "MEMORANDUM", "FUND", "NOTICE", "REGULATORY"]):
            return "heading"
        
        if re.match(r'^\s*[-•▪▫]\s+', text) or re.match(r'^\s*\d+\.\s+', text):
            return "list_item"
        
        if len(re.findall(r'\b\d+\b', text)) > 3 and ('\t' in text or '  ' in text):
            return "table_data"
        
        if len(text) < 200 and (text.startswith('*') or text.startswith('Note:')):
            return "footnote"
        
        if len(text) > 100:
            return "paragraph"
        
        return "text_block"

    def _classify_image_type(self, img_data: Dict) -> str:
        """Classifica o tipo de imagem baseado nas características."""
        width = img_data.get("width", 0)
        height = img_data.get("height", 0)
        pixels = width * height
        aspect_ratio = width / height if height > 0 else 1
        
        if (pixels > 50000 and 
            0.5 <= aspect_ratio <= 3.0 and
            width > 300 and height > 200):
            return "chart"
        
        if pixels > 20000 and aspect_ratio > 2.5:
            return "diagram"
        
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
            
            if (re.search(r'\d+.*\d+.*\d+', line) or 
                '\t' in line or 
                len(re.findall(r'\s{3,}', line)) >= 2):
                
                current_table.append({
                    "line_number": i + 1,
                    "content": line
                })
            else:
                if len(current_table) >= 2:
                    tables.append({
                        "id": f"table_{len(tables) + 1}",
                        "start_line": current_table[0]["line_number"],
                        "end_line": current_table[-1]["line_number"],
                        "rows": current_table,
                        "row_count": len(current_table)
                    })
                current_table = []
        
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
        print(f"📦 Criando chunks de conteúdo...")
        
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
        
        for page_elements in all_elements:
            page_num = page_elements["page_number"]
            print(f"📄 Processando página {page_num} para chunks...")
            
            visual_context = ""
            if page_elements["images"]:
                visual_context += f"\n[PÁGINA {page_num} CONTÉM {len(page_elements['images'])} IMAGENS]"
                for img in page_elements["images"]:
                    if img.get("likely_type") in ["chart", "graph"]:
                        visual_context += f"\n[GRÁFICO: {img['dimensions']}]"
            
            if page_elements["tables"]:
                visual_context += f"\n[PÁGINA {page_num} CONTÉM {len(page_elements['tables'])} TABELAS]"
            
            for block in page_elements["structured_blocks"]:
                block_content = block["content"]
                block_type = block["type"]
                
                content_with_context = block_content
                if visual_context and block_type in ["heading", "paragraph"]:
                    content_with_context = block_content + visual_context
                    visual_context = ""
                
                if (len(current_chunk["content"]) + len(content_with_context) > self.chunk_size and 
                    len(current_chunk["content"]) > self.min_chunk_size):
                    
                    self._finalize_chunk(current_chunk, chunk_counter)
                    chunks.append(current_chunk)
                    
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
                
                if current_chunk["content"]:
                    current_chunk["content"] += "\n\n"
                current_chunk["content"] += content_with_context
                
                if page_num not in current_chunk["metadata"]["pages"]:
                    current_chunk["metadata"]["pages"].append(page_num)
                current_chunk["metadata"]["elements"].append(block["id"])
                if block_type not in current_chunk["metadata"]["content_types"]:
                    current_chunk["metadata"]["content_types"].append(block_type)
        
        if current_chunk["content"].strip():
            self._finalize_chunk(current_chunk, chunk_counter)
            chunks.append(current_chunk)
        
        print(f"✅ Criados {len(chunks)} chunks de conteúdo")
        
        for i, chunk in enumerate(chunks):
            chunk["context"]["chunk_position"] = f"{i + 1}/{len(chunks)}"
            if i > 0:
                chunk["context"]["previous_chunk_id"] = chunks[i - 1]["id"]
            if i < len(chunks) - 1:
                chunk["context"]["next_chunk_id"] = chunks[i + 1]["id"]
        
        return chunks

    def _finalize_chunk(self, chunk: Dict, chunk_id: int):
        """Finaliza um chunk calculando metadados."""
        chunk["id"] = f"chunk_{chunk_id}"
        chunk["metadata"]["word_count"] = len(chunk["content"].split())
        chunk["metadata"]["char_count"] = len(chunk["content"])

    def _get_overlap_content(self, content: str) -> str:
        """Obtém conteúdo de sobreposição do chunk anterior."""
        if len(content) <= self.overlap:
            return content
        
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
        
        summary = ' '.join(words[:10]) + " ... " + ' '.join(words[-10:])
        return summary

    def _get_section_context(self, content: str) -> str:
        """Identifica o contexto da seção atual."""
        content_upper = content.upper()
        
        if any(keyword in content_upper for keyword in ["CONFIDENTIAL", "MEMORANDUM"]):
            return "document_header"
        elif any(keyword in content_upper for keyword in ["RISK", "WARNING", "CAUTION"]):
            return "risk_section"
        elif any(keyword in content_upper for keyword in ["INVESTMENT", "FUND", "PORTFOLIO"]):
            return "investment_section"
        elif any(keyword in content_upper for keyword in ["LEGAL", "REGULATORY", "COMPLIANCE"]):
            return "legal_section"
        elif re.search(r'\d+\.\d+%|\$\d+|USD|EUR', content):
            return "financial_data"
        else:
            return "general_content"
    
    def extract_to_chunks(self, file_path: str, output_dir: str = "C:/extrair", 
                         fund_identifier: str = None, map_id: int = None) -> Dict[str, Any]:
        """Extração principal para chunks contextuais com dados SQL."""
        
        file_path = Path(file_path)
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"EXTRAÇÃO PDF PARA CHUNKS CONTEXTUAIS COM SQL")
        print(f"Arquivo: {file_path.name}")
        print(f"Saída: {output_dir}")
        
        if map_id:
            print(f"MapID: {map_id}")
        elif fund_identifier:
            print(f"Identificador do fundo: {fund_identifier}")
        else:
            print(f"Identificador do fundo: Auto-detectar")
        
        try:
            doc = fitz.open(str(file_path))
            print(f"Total de páginas: {len(doc)}")
            
            # 1. Extrair metadados com integração SQL
            doc_metadata = self.extract_document_metadata(doc, fund_identifier, map_id)
            
            # 2. Extrair elementos de todas as páginas
            all_elements = []
            for page_num in range(len(doc)):
                page_elements = self.extract_page_elements(doc, page_num)
                all_elements.append(page_elements)
            
            # 3. Criar chunks contextuais
            content_chunks = self.create_content_chunks(all_elements)
            
            # 4. Estrutura final dos dados com informações SQL
            extracted_data = {
                "document_info": {
                    "filename": file_path.name,
                    "source_path": str(file_path),
                    "metadata": doc_metadata,
                    "extraction_config": {
                        "chunk_size": self.chunk_size,
                        "overlap": self.overlap,
                        "extraction_timestamp": str(datetime.datetime.now()),
                        "fund_identifier_used": fund_identifier,
                        "map_id_used": map_id
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
                "page_elements": all_elements
            }
            
            # 5. Salvar resultado
            output_file = output_dir / f"{file_path.stem}_chunks.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(extracted_data, f, indent=2, ensure_ascii=False)
            
            # 6. Mostrar resumo com dados SQL
            print(f"\nEXTRAÇÃO CONCLUÍDA!")
            print(f"Estatísticas:")
            print(f" - Chunks criados: {extracted_data['summary']['total_chunks']}")
            print(f" - Palavras totais: {extracted_data['summary']['total_words']:,}")
            print(f" - Imagens detectadas: {extracted_data['summary']['total_images']}")
            print(f" - Tabelas detectadas: {extracted_data['summary']['total_tables']}")
            
            # Mostrar informações SQL se disponível
            sql_info = doc_metadata.get("fund_database_info", {})
            if sql_info.get("sql_data_available", False):
                print(f"\nINFORMAÇÕES DO FUNDO (SQL):")
                print(f" - MapID: {sql_info.get('map_id', 'N/A')}")
                print(f" - Gestor: {sql_info.get('management_company', 'N/A')}")
                print(f" - Fundo: {sql_info.get('fund_name', 'N/A')}")
                print(f" - Nome curto: {sql_info.get('fund_short_name', 'N/A')}")
                print(f" - Asset class: {sql_info.get('asset_class_report', 'N/A')}")
                print(f" - Moeda: {sql_info.get('return_currency', 'N/A')}")
            else:
                print(f"\nInformações SQL não disponíveis")
                if sql_info.get("total_records_found", 0) > 1:
                    print(f"Sugestão: Use MapID específico para seleção precisa")
            
            print(f"\nArquivo salvo: {output_file}")
            
            doc.close()
            return extracted_data
            
        except Exception as e:
            print(f"Erro na extração: {e}")
            return None


def main():
    """Função principal com suporte a MapID e identificador de fundo."""
    print("Extrator PDF para Chunks Contextuais - Com Integração SQL")
    
    if len(sys.argv) < 2:
        print("\nCOMO USAR:")
        print(" python pdf_extractor.py <arquivo.pdf>")
        print(" python pdf_extractor.py <arquivo.pdf> <fund_identifier>")
        print(" python pdf_extractor.py <arquivo.pdf> --map-id <MapID>")
        print(" python pdf_extractor.py <arquivo.pdf> --map-id <MapID> <chunk_size>")
        print("\nExemplos:")
        print(" python pdf_extractor.py documento.pdf")
        print(" python pdf_extractor.py documento.pdf 'Pershing Square'")
        print(" python pdf_extractor.py documento.pdf --map-id 123")
        print(" python pdf_extractor.py documento.pdf --map-id 123 1500")
        return

    pdf_file = sys.argv[1]
    fund_identifier = None
    map_id = None
    chunk_size = 1000
    overlap = 200
    
    # Processar argumentos
    i = 2
    while i < len(sys.argv):
        if sys.argv[i] == "--map-id" and i + 1 < len(sys.argv):
            try:
                map_id = int(sys.argv[i + 1])
                i += 2
            except ValueError:
                print(f"Erro: MapID deve ser um número inteiro: {sys.argv[i + 1]}")
                return
        elif sys.argv[i].isdigit() and not map_id:  # chunk_size
            chunk_size = int(sys.argv[i])
            i += 1
        elif not fund_identifier and not map_id:  # fund_identifier
            fund_identifier = sys.argv[i]
            i += 1
        else:
            i += 1
    
    if not Path(pdf_file).exists():
        print(f"Arquivo não encontrado: {pdf_file}")
        return
    
    print(f"\nParametros:")
    print(f" - Arquivo: {pdf_file}")
    if map_id:
        print(f" - MapID: {map_id}")
    elif fund_identifier:
        print(f" - Identificador: {fund_identifier}")
    else:
        print(f" - Detecção automática ativada")
    print(f" - Chunk size: {chunk_size}")
    
    # Criar extrator e executar
    extractor = PDFToChunksExtractor(chunk_size=chunk_size, overlap=overlap)
    result = extractor.extract_to_chunks(pdf_file, fund_identifier=fund_identifier, map_id=map_id)
    
    if result:
        print(f"\nRESULTADO OTIMIZADO PARA LLM!")
        print(f"{result['summary']['total_chunks']} chunks prontos para análise")
        print(f"Cada chunk mantém contexto + informações do fundo do SQL")


if __name__ == "__main__":
    print("\nEXEMPLO PARA SEU ARQUIVO:")
    print(" python pdf_extractor.py C:\\extrair\\paginas.pdf --map-id 123")
    print(" python pdf_extractor.py C:\\extrair\\paginas.pdf 'Pershing Square'")
    print()
    main(),                 # Só números (páginas)
            r'^page\s+\d+\s*
        """Classifica o tipo de bloco baseado no conteúdo e formatação."""
        text_upper = text.upper()
        
        if any(keyword in text_upper for keyword in 
               ["CONFIDENTIAL", "MEMORANDUM", "FUND", "NOTICE", "REGULATORY"]):
            return "heading"
        
        if re.match(r'^\s*[-•▪▫]\s+', text) or re.match(r'^\s*\d+\.\s+', text):
            return "list_item"
        
        if len(re.findall(r'\b\d+\b', text)) > 3 and ('\t' in text or '  ' in text):
            return "table_data"
        
        if len(text) < 200 and (text.startswith('*') or text.startswith('Note:')):
            return "footnote"
        
        if len(text) > 100:
            return "paragraph"
        
        return "text_block"

    def _classify_image_type(self, img_data: Dict) -> str:
        """Classifica o tipo de imagem baseado nas características."""
        width = img_data.get("width", 0)
        height = img_data.get("height", 0)
        pixels = width * height
        aspect_ratio = width / height if height > 0 else 1
        
        if (pixels > 50000 and 
            0.5 <= aspect_ratio <= 3.0 and
            width > 300 and height > 200):
            return "chart"
        
        if pixels > 20000 and aspect_ratio > 2.5:
            return "diagram"
        
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
            
            if (re.search(r'\d+.*\d+.*\d+', line) or 
                '\t' in line or 
                len(re.findall(r'\s{3,}', line)) >= 2):
                
                current_table.append({
                    "line_number": i + 1,
                    "content": line
                })
            else:
                if len(current_table) >= 2:
                    tables.append({
                        "id": f"table_{len(tables) + 1}",
                        "start_line": current_table[0]["line_number"],
                        "end_line": current_table[-1]["line_number"],
                        "rows": current_table,
                        "row_count": len(current_table)
                    })
                current_table = []
        
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
        print(f"📦 Criando chunks de conteúdo...")
        
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
        
        for page_elements in all_elements:
            page_num = page_elements["page_number"]
            print(f"📄 Processando página {page_num} para chunks...")
            
            visual_context = ""
            if page_elements["images"]:
                visual_context += f"\n[PÁGINA {page_num} CONTÉM {len(page_elements['images'])} IMAGENS]"
                for img in page_elements["images"]:
                    if img.get("likely_type") in ["chart", "graph"]:
                        visual_context += f"\n[GRÁFICO: {img['dimensions']}]"
            
            if page_elements["tables"]:
                visual_context += f"\n[PÁGINA {page_num} CONTÉM {len(page_elements['tables'])} TABELAS]"
            
            for block in page_elements["structured_blocks"]:
                block_content = block["content"]
                block_type = block["type"]
                
                content_with_context = block_content
                if visual_context and block_type in ["heading", "paragraph"]:
                    content_with_context = block_content + visual_context
                    visual_context = ""
                
                if (len(current_chunk["content"]) + len(content_with_context) > self.chunk_size and 
                    len(current_chunk["content"]) > self.min_chunk_size):
                    
                    self._finalize_chunk(current_chunk, chunk_counter)
                    chunks.append(current_chunk)
                    
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
                
                if current_chunk["content"]:
                    current_chunk["content"] += "\n\n"
                current_chunk["content"] += content_with_context
                
                if page_num not in current_chunk["metadata"]["pages"]:
                    current_chunk["metadata"]["pages"].append(page_num)
                current_chunk["metadata"]["elements"].append(block["id"])
                if block_type not in current_chunk["metadata"]["content_types"]:
                    current_chunk["metadata"]["content_types"].append(block_type)
        
        if current_chunk["content"].strip():
            self._finalize_chunk(current_chunk, chunk_counter)
            chunks.append(current_chunk)
        
        print(f"✅ Criados {len(chunks)} chunks de conteúdo")
        
        for i, chunk in enumerate(chunks):
            chunk["context"]["chunk_position"] = f"{i + 1}/{len(chunks)}"
            if i > 0:
                chunk["context"]["previous_chunk_id"] = chunks[i - 1]["id"]
            if i < len(chunks) - 1:
                chunk["context"]["next_chunk_id"] = chunks[i + 1]["id"]
        
        return chunks

    def _finalize_chunk(self, chunk: Dict, chunk_id: int):
        """Finaliza um chunk calculando metadados."""
        chunk["id"] = f"chunk_{chunk_id}"
        chunk["metadata"]["word_count"] = len(chunk["content"].split())
        chunk["metadata"]["char_count"] = len(chunk["content"])

    def _get_overlap_content(self, content: str) -> str:
        """Obtém conteúdo de sobreposição do chunk anterior."""
        if len(content) <= self.overlap:
            return content
        
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
        
        summary = ' '.join(words[:10]) + " ... " + ' '.join(words[-10:])
        return summary

    def _get_section_context(self, content: str) -> str:
        """Identifica o contexto da seção atual."""
        content_upper = content.upper()
        
        if any(keyword in content_upper for keyword in ["CONFIDENTIAL", "MEMORANDUM"]):
            return "document_header"
        elif any(keyword in content_upper for keyword in ["RISK", "WARNING", "CAUTION"]):
            return "risk_section"
        elif any(keyword in content_upper for keyword in ["INVESTMENT", "FUND", "PORTFOLIO"]):
            return "investment_section"
        elif any(keyword in content_upper for keyword in ["LEGAL", "REGULATORY", "COMPLIANCE"]):
            return "legal_section"
        elif re.search(r'\d+\.\d+%|\$\d+|USD|EUR', content):
            return "financial_data"
        else:
            return "general_content"
    
    def extract_to_chunks(self, file_path: str, output_dir: str = "C:/extrair", 
                         fund_identifier: str = None, map_id: int = None) -> Dict[str, Any]:
        """Extração principal para chunks contextuais com dados SQL."""
        
        file_path = Path(file_path)
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"EXTRAÇÃO PDF PARA CHUNKS CONTEXTUAIS COM SQL")
        print(f"Arquivo: {file_path.name}")
        print(f"Saída: {output_dir}")
        
        if map_id:
            print(f"MapID: {map_id}")
        elif fund_identifier:
            print(f"Identificador do fundo: {fund_identifier}")
        else:
            print(f"Identificador do fundo: Auto-detectar")
        
        try:
            doc = fitz.open(str(file_path))
            print(f"Total de páginas: {len(doc)}")
            
            # 1. Extrair metadados com integração SQL
            doc_metadata = self.extract_document_metadata(doc, fund_identifier, map_id)
            
            # 2. Extrair elementos de todas as páginas
            all_elements = []
            for page_num in range(len(doc)):
                page_elements = self.extract_page_elements(doc, page_num)
                all_elements.append(page_elements)
            
            # 3. Criar chunks contextuais
            content_chunks = self.create_content_chunks(all_elements)
            
            # 4. Estrutura final dos dados com informações SQL
            extracted_data = {
                "document_info": {
                    "filename": file_path.name,
                    "source_path": str(file_path),
                    "metadata": doc_metadata,
                    "extraction_config": {
                        "chunk_size": self.chunk_size,
                        "overlap": self.overlap,
                        "extraction_timestamp": str(datetime.datetime.now()),
                        "fund_identifier_used": fund_identifier,
                        "map_id_used": map_id
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
                "page_elements": all_elements
            }
            
            # 5. Salvar resultado
            output_file = output_dir / f"{file_path.stem}_chunks.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(extracted_data, f, indent=2, ensure_ascii=False)
            
            # 6. Mostrar resumo com dados SQL
            print(f"\nEXTRAÇÃO CONCLUÍDA!")
            print(f"Estatísticas:")
            print(f" - Chunks criados: {extracted_data['summary']['total_chunks']}")
            print(f" - Palavras totais: {extracted_data['summary']['total_words']:,}")
            print(f" - Imagens detectadas: {extracted_data['summary']['total_images']}")
            print(f" - Tabelas detectadas: {extracted_data['summary']['total_tables']}")
            
            # Mostrar informações SQL se disponível
            sql_info = doc_metadata.get("fund_database_info", {})
            if sql_info.get("sql_data_available", False):
                print(f"\nINFORMAÇÕES DO FUNDO (SQL):")
                print(f" - MapID: {sql_info.get('map_id', 'N/A')}")
                print(f" - Gestor: {sql_info.get('management_company', 'N/A')}")
                print(f" - Fundo: {sql_info.get('fund_name', 'N/A')}")
                print(f" - Nome curto: {sql_info.get('fund_short_name', 'N/A')}")
                print(f" - Asset class: {sql_info.get('asset_class_report', 'N/A')}")
                print(f" - Moeda: {sql_info.get('return_currency', 'N/A')}")
            else:
                print(f"\nInformações SQL não disponíveis")
                if sql_info.get("total_records_found", 0) > 1:
                    print(f"Sugestão: Use MapID específico para seleção precisa")
            
            print(f"\nArquivo salvo: {output_file}")
            
            doc.close()
            return extracted_data
            
        except Exception as e:
            print(f"Erro na extração: {e}")
            return None


def main():
    """Função principal com suporte a MapID e identificador de fundo."""
    print("Extrator PDF para Chunks Contextuais - Com Integração SQL")
    
    if len(sys.argv) < 2:
        print("\nCOMO USAR:")
        print(" python pdf_extractor.py <arquivo.pdf>")
        print(" python pdf_extractor.py <arquivo.pdf> <fund_identifier>")
        print(" python pdf_extractor.py <arquivo.pdf> --map-id <MapID>")
        print(" python pdf_extractor.py <arquivo.pdf> --map-id <MapID> <chunk_size>")
        print("\nExemplos:")
        print(" python pdf_extractor.py documento.pdf")
        print(" python pdf_extractor.py documento.pdf 'Pershing Square'")
        print(" python pdf_extractor.py documento.pdf --map-id 123")
        print(" python pdf_extractor.py documento.pdf --map-id 123 1500")
        return

    pdf_file = sys.argv[1]
    fund_identifier = None
    map_id = None
    chunk_size = 1000
    overlap = 200
    
    # Processar argumentos
    i = 2
    while i < len(sys.argv):
        if sys.argv[i] == "--map-id" and i + 1 < len(sys.argv):
            try:
                map_id = int(sys.argv[i + 1])
                i += 2
            except ValueError:
                print(f"Erro: MapID deve ser um número inteiro: {sys.argv[i + 1]}")
                return
        elif sys.argv[i].isdigit() and not map_id:  # chunk_size
            chunk_size = int(sys.argv[i])
            i += 1
        elif not fund_identifier and not map_id:  # fund_identifier
            fund_identifier = sys.argv[i]
            i += 1
        else:
            i += 1
    
    if not Path(pdf_file).exists():
        print(f"Arquivo não encontrado: {pdf_file}")
        return
    
    print(f"\nParametros:")
    print(f" - Arquivo: {pdf_file}")
    if map_id:
        print(f" - MapID: {map_id}")
    elif fund_identifier:
        print(f" - Identificador: {fund_identifier}")
    else:
        print(f" - Detecção automática ativada")
    print(f" - Chunk size: {chunk_size}")
    
    # Criar extrator e executar
    extractor = PDFToChunksExtractor(chunk_size=chunk_size, overlap=overlap)
    result = extractor.extract_to_chunks(pdf_file, fund_identifier=fund_identifier, map_id=map_id)
    
    if result:
        print(f"\nRESULTADO OTIMIZADO PARA LLM!")
        print(f"{result['summary']['total_chunks']} chunks prontos para análise")
        print(f"Cada chunk mantém contexto + informações do fundo do SQL")


if __name__ == "__main__":
    print("\nEXEMPLO PARA SEU ARQUIVO:")
    print(" python pdf_extractor.py C:\\extrair\\paginas.pdf --map-id 123")
    print(" python pdf_extractor.py C:\\extrair\\paginas.pdf 'Pershing Square'")
    print()
    main(),          # "page 1"
            r'^[a-z]\s*
        """Classifica o tipo de bloco baseado no conteúdo e formatação."""
        text_upper = text.upper()
        
        if any(keyword in text_upper for keyword in 
               ["CONFIDENTIAL", "MEMORANDUM", "FUND", "NOTICE", "REGULATORY"]):
            return "heading"
        
        if re.match(r'^\s*[-•▪▫]\s+', text) or re.match(r'^\s*\d+\.\s+', text):
            return "list_item"
        
        if len(re.findall(r'\b\d+\b', text)) > 3 and ('\t' in text or '  ' in text):
            return "table_data"
        
        if len(text) < 200 and (text.startswith('*') or text.startswith('Note:')):
            return "footnote"
        
        if len(text) > 100:
            return "paragraph"
        
        return "text_block"

    def _classify_image_type(self, img_data: Dict) -> str:
        """Classifica o tipo de imagem baseado nas características."""
        width = img_data.get("width", 0)
        height = img_data.get("height", 0)
        pixels = width * height
        aspect_ratio = width / height if height > 0 else 1
        
        if (pixels > 50000 and 
            0.5 <= aspect_ratio <= 3.0 and
            width > 300 and height > 200):
            return "chart"
        
        if pixels > 20000 and aspect_ratio > 2.5:
            return "diagram"
        
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
            
            if (re.search(r'\d+.*\d+.*\d+', line) or 
                '\t' in line or 
                len(re.findall(r'\s{3,}', line)) >= 2):
                
                current_table.append({
                    "line_number": i + 1,
                    "content": line
                })
            else:
                if len(current_table) >= 2:
                    tables.append({
                        "id": f"table_{len(tables) + 1}",
                        "start_line": current_table[0]["line_number"],
                        "end_line": current_table[-1]["line_number"],
                        "rows": current_table,
                        "row_count": len(current_table)
                    })
                current_table = []
        
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
        print(f"📦 Criando chunks de conteúdo...")
        
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
        
        for page_elements in all_elements:
            page_num = page_elements["page_number"]
            print(f"📄 Processando página {page_num} para chunks...")
            
            visual_context = ""
            if page_elements["images"]:
                visual_context += f"\n[PÁGINA {page_num} CONTÉM {len(page_elements['images'])} IMAGENS]"
                for img in page_elements["images"]:
                    if img.get("likely_type") in ["chart", "graph"]:
                        visual_context += f"\n[GRÁFICO: {img['dimensions']}]"
            
            if page_elements["tables"]:
                visual_context += f"\n[PÁGINA {page_num} CONTÉM {len(page_elements['tables'])} TABELAS]"
            
            for block in page_elements["structured_blocks"]:
                block_content = block["content"]
                block_type = block["type"]
                
                content_with_context = block_content
                if visual_context and block_type in ["heading", "paragraph"]:
                    content_with_context = block_content + visual_context
                    visual_context = ""
                
                if (len(current_chunk["content"]) + len(content_with_context) > self.chunk_size and 
                    len(current_chunk["content"]) > self.min_chunk_size):
                    
                    self._finalize_chunk(current_chunk, chunk_counter)
                    chunks.append(current_chunk)
                    
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
                
                if current_chunk["content"]:
                    current_chunk["content"] += "\n\n"
                current_chunk["content"] += content_with_context
                
                if page_num not in current_chunk["metadata"]["pages"]:
                    current_chunk["metadata"]["pages"].append(page_num)
                current_chunk["metadata"]["elements"].append(block["id"])
                if block_type not in current_chunk["metadata"]["content_types"]:
                    current_chunk["metadata"]["content_types"].append(block_type)
        
        if current_chunk["content"].strip():
            self._finalize_chunk(current_chunk, chunk_counter)
            chunks.append(current_chunk)
        
        print(f"✅ Criados {len(chunks)} chunks de conteúdo")
        
        for i, chunk in enumerate(chunks):
            chunk["context"]["chunk_position"] = f"{i + 1}/{len(chunks)}"
            if i > 0:
                chunk["context"]["previous_chunk_id"] = chunks[i - 1]["id"]
            if i < len(chunks) - 1:
                chunk["context"]["next_chunk_id"] = chunks[i + 1]["id"]
        
        return chunks

    def _finalize_chunk(self, chunk: Dict, chunk_id: int):
        """Finaliza um chunk calculando metadados."""
        chunk["id"] = f"chunk_{chunk_id}"
        chunk["metadata"]["word_count"] = len(chunk["content"].split())
        chunk["metadata"]["char_count"] = len(chunk["content"])

    def _get_overlap_content(self, content: str) -> str:
        """Obtém conteúdo de sobreposição do chunk anterior."""
        if len(content) <= self.overlap:
            return content
        
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
        
        summary = ' '.join(words[:10]) + " ... " + ' '.join(words[-10:])
        return summary

    def _get_section_context(self, content: str) -> str:
        """Identifica o contexto da seção atual."""
        content_upper = content.upper()
        
        if any(keyword in content_upper for keyword in ["CONFIDENTIAL", "MEMORANDUM"]):
            return "document_header"
        elif any(keyword in content_upper for keyword in ["RISK", "WARNING", "CAUTION"]):
            return "risk_section"
        elif any(keyword in content_upper for keyword in ["INVESTMENT", "FUND", "PORTFOLIO"]):
            return "investment_section"
        elif any(keyword in content_upper for keyword in ["LEGAL", "REGULATORY", "COMPLIANCE"]):
            return "legal_section"
        elif re.search(r'\d+\.\d+%|\$\d+|USD|EUR', content):
            return "financial_data"
        else:
            return "general_content"
    
    def extract_to_chunks(self, file_path: str, output_dir: str = "C:/extrair", 
                         fund_identifier: str = None, map_id: int = None) -> Dict[str, Any]:
        """Extração principal para chunks contextuais com dados SQL."""
        
        file_path = Path(file_path)
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"EXTRAÇÃO PDF PARA CHUNKS CONTEXTUAIS COM SQL")
        print(f"Arquivo: {file_path.name}")
        print(f"Saída: {output_dir}")
        
        if map_id:
            print(f"MapID: {map_id}")
        elif fund_identifier:
            print(f"Identificador do fundo: {fund_identifier}")
        else:
            print(f"Identificador do fundo: Auto-detectar")
        
        try:
            doc = fitz.open(str(file_path))
            print(f"Total de páginas: {len(doc)}")
            
            # 1. Extrair metadados com integração SQL
            doc_metadata = self.extract_document_metadata(doc, fund_identifier, map_id)
            
            # 2. Extrair elementos de todas as páginas
            all_elements = []
            for page_num in range(len(doc)):
                page_elements = self.extract_page_elements(doc, page_num)
                all_elements.append(page_elements)
            
            # 3. Criar chunks contextuais
            content_chunks = self.create_content_chunks(all_elements)
            
            # 4. Estrutura final dos dados com informações SQL
            extracted_data = {
                "document_info": {
                    "filename": file_path.name,
                    "source_path": str(file_path),
                    "metadata": doc_metadata,
                    "extraction_config": {
                        "chunk_size": self.chunk_size,
                        "overlap": self.overlap,
                        "extraction_timestamp": str(datetime.datetime.now()),
                        "fund_identifier_used": fund_identifier,
                        "map_id_used": map_id
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
                "page_elements": all_elements
            }
            
            # 5. Salvar resultado
            output_file = output_dir / f"{file_path.stem}_chunks.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(extracted_data, f, indent=2, ensure_ascii=False)
            
            # 6. Mostrar resumo com dados SQL
            print(f"\nEXTRAÇÃO CONCLUÍDA!")
            print(f"Estatísticas:")
            print(f" - Chunks criados: {extracted_data['summary']['total_chunks']}")
            print(f" - Palavras totais: {extracted_data['summary']['total_words']:,}")
            print(f" - Imagens detectadas: {extracted_data['summary']['total_images']}")
            print(f" - Tabelas detectadas: {extracted_data['summary']['total_tables']}")
            
            # Mostrar informações SQL se disponível
            sql_info = doc_metadata.get("fund_database_info", {})
            if sql_info.get("sql_data_available", False):
                print(f"\nINFORMAÇÕES DO FUNDO (SQL):")
                print(f" - MapID: {sql_info.get('map_id', 'N/A')}")
                print(f" - Gestor: {sql_info.get('management_company', 'N/A')}")
                print(f" - Fundo: {sql_info.get('fund_name', 'N/A')}")
                print(f" - Nome curto: {sql_info.get('fund_short_name', 'N/A')}")
                print(f" - Asset class: {sql_info.get('asset_class_report', 'N/A')}")
                print(f" - Moeda: {sql_info.get('return_currency', 'N/A')}")
            else:
                print(f"\nInformações SQL não disponíveis")
                if sql_info.get("total_records_found", 0) > 1:
                    print(f"Sugestão: Use MapID específico para seleção precisa")
            
            print(f"\nArquivo salvo: {output_file}")
            
            doc.close()
            return extracted_data
            
        except Exception as e:
            print(f"Erro na extração: {e}")
            return None


def main():
    """Função principal com suporte a MapID e identificador de fundo."""
    print("Extrator PDF para Chunks Contextuais - Com Integração SQL")
    
    if len(sys.argv) < 2:
        print("\nCOMO USAR:")
        print(" python pdf_extractor.py <arquivo.pdf>")
        print(" python pdf_extractor.py <arquivo.pdf> <fund_identifier>")
        print(" python pdf_extractor.py <arquivo.pdf> --map-id <MapID>")
        print(" python pdf_extractor.py <arquivo.pdf> --map-id <MapID> <chunk_size>")
        print("\nExemplos:")
        print(" python pdf_extractor.py documento.pdf")
        print(" python pdf_extractor.py documento.pdf 'Pershing Square'")
        print(" python pdf_extractor.py documento.pdf --map-id 123")
        print(" python pdf_extractor.py documento.pdf --map-id 123 1500")
        return

    pdf_file = sys.argv[1]
    fund_identifier = None
    map_id = None
    chunk_size = 1000
    overlap = 200
    
    # Processar argumentos
    i = 2
    while i < len(sys.argv):
        if sys.argv[i] == "--map-id" and i + 1 < len(sys.argv):
            try:
                map_id = int(sys.argv[i + 1])
                i += 2
            except ValueError:
                print(f"Erro: MapID deve ser um número inteiro: {sys.argv[i + 1]}")
                return
        elif sys.argv[i].isdigit() and not map_id:  # chunk_size
            chunk_size = int(sys.argv[i])
            i += 1
        elif not fund_identifier and not map_id:  # fund_identifier
            fund_identifier = sys.argv[i]
            i += 1
        else:
            i += 1
    
    if not Path(pdf_file).exists():
        print(f"Arquivo não encontrado: {pdf_file}")
        return
    
    print(f"\nParametros:")
    print(f" - Arquivo: {pdf_file}")
    if map_id:
        print(f" - MapID: {map_id}")
    elif fund_identifier:
        print(f" - Identificador: {fund_identifier}")
    else:
        print(f" - Detecção automática ativada")
    print(f" - Chunk size: {chunk_size}")
    
    # Criar extrator e executar
    extractor = PDFToChunksExtractor(chunk_size=chunk_size, overlap=overlap)
    result = extractor.extract_to_chunks(pdf_file, fund_identifier=fund_identifier, map_id=map_id)
    
    if result:
        print(f"\nRESULTADO OTIMIZADO PARA LLM!")
        print(f"{result['summary']['total_chunks']} chunks prontos para análise")
        print(f"Cada chunk mantém contexto + informações do fundo do SQL")


if __name__ == "__main__":
    print("\nEXEMPLO PARA SEU ARQUIVO:")
    print(" python pdf_extractor.py C:\\extrair\\paginas.pdf --map-id 123")
    print(" python pdf_extractor.py C:\\extrair\\paginas.pdf 'Pershing Square'")
    print()
    main(),               # Uma letra só
            r'^\s*\|\s*
        """Classifica o tipo de bloco baseado no conteúdo e formatação."""
        text_upper = text.upper()
        
        if any(keyword in text_upper for keyword in 
               ["CONFIDENTIAL", "MEMORANDUM", "FUND", "NOTICE", "REGULATORY"]):
            return "heading"
        
        if re.match(r'^\s*[-•▪▫]\s+', text) or re.match(r'^\s*\d+\.\s+', text):
            return "list_item"
        
        if len(re.findall(r'\b\d+\b', text)) > 3 and ('\t' in text or '  ' in text):
            return "table_data"
        
        if len(text) < 200 and (text.startswith('*') or text.startswith('Note:')):
            return "footnote"
        
        if len(text) > 100:
            return "paragraph"
        
        return "text_block"

    def _classify_image_type(self, img_data: Dict) -> str:
        """Classifica o tipo de imagem baseado nas características."""
        width = img_data.get("width", 0)
        height = img_data.get("height", 0)
        pixels = width * height
        aspect_ratio = width / height if height > 0 else 1
        
        if (pixels > 50000 and 
            0.5 <= aspect_ratio <= 3.0 and
            width > 300 and height > 200):
            return "chart"
        
        if pixels > 20000 and aspect_ratio > 2.5:
            return "diagram"
        
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
            
            if (re.search(r'\d+.*\d+.*\d+', line) or 
                '\t' in line or 
                len(re.findall(r'\s{3,}', line)) >= 2):
                
                current_table.append({
                    "line_number": i + 1,
                    "content": line
                })
            else:
                if len(current_table) >= 2:
                    tables.append({
                        "id": f"table_{len(tables) + 1}",
                        "start_line": current_table[0]["line_number"],
                        "end_line": current_table[-1]["line_number"],
                        "rows": current_table,
                        "row_count": len(current_table)
                    })
                current_table = []
        
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
        print(f"📦 Criando chunks de conteúdo...")
        
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
        
        for page_elements in all_elements:
            page_num = page_elements["page_number"]
            print(f"📄 Processando página {page_num} para chunks...")
            
            visual_context = ""
            if page_elements["images"]:
                visual_context += f"\n[PÁGINA {page_num} CONTÉM {len(page_elements['images'])} IMAGENS]"
                for img in page_elements["images"]:
                    if img.get("likely_type") in ["chart", "graph"]:
                        visual_context += f"\n[GRÁFICO: {img['dimensions']}]"
            
            if page_elements["tables"]:
                visual_context += f"\n[PÁGINA {page_num} CONTÉM {len(page_elements['tables'])} TABELAS]"
            
            for block in page_elements["structured_blocks"]:
                block_content = block["content"]
                block_type = block["type"]
                
                content_with_context = block_content
                if visual_context and block_type in ["heading", "paragraph"]:
                    content_with_context = block_content + visual_context
                    visual_context = ""
                
                if (len(current_chunk["content"]) + len(content_with_context) > self.chunk_size and 
                    len(current_chunk["content"]) > self.min_chunk_size):
                    
                    self._finalize_chunk(current_chunk, chunk_counter)
                    chunks.append(current_chunk)
                    
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
                
                if current_chunk["content"]:
                    current_chunk["content"] += "\n\n"
                current_chunk["content"] += content_with_context
                
                if page_num not in current_chunk["metadata"]["pages"]:
                    current_chunk["metadata"]["pages"].append(page_num)
                current_chunk["metadata"]["elements"].append(block["id"])
                if block_type not in current_chunk["metadata"]["content_types"]:
                    current_chunk["metadata"]["content_types"].append(block_type)
        
        if current_chunk["content"].strip():
            self._finalize_chunk(current_chunk, chunk_counter)
            chunks.append(current_chunk)
        
        print(f"✅ Criados {len(chunks)} chunks de conteúdo")
        
        for i, chunk in enumerate(chunks):
            chunk["context"]["chunk_position"] = f"{i + 1}/{len(chunks)}"
            if i > 0:
                chunk["context"]["previous_chunk_id"] = chunks[i - 1]["id"]
            if i < len(chunks) - 1:
                chunk["context"]["next_chunk_id"] = chunks[i + 1]["id"]
        
        return chunks

    def _finalize_chunk(self, chunk: Dict, chunk_id: int):
        """Finaliza um chunk calculando metadados."""
        chunk["id"] = f"chunk_{chunk_id}"
        chunk["metadata"]["word_count"] = len(chunk["content"].split())
        chunk["metadata"]["char_count"] = len(chunk["content"])

    def _get_overlap_content(self, content: str) -> str:
        """Obtém conteúdo de sobreposição do chunk anterior."""
        if len(content) <= self.overlap:
            return content
        
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
        
        summary = ' '.join(words[:10]) + " ... " + ' '.join(words[-10:])
        return summary

    def _get_section_context(self, content: str) -> str:
        """Identifica o contexto da seção atual."""
        content_upper = content.upper()
        
        if any(keyword in content_upper for keyword in ["CONFIDENTIAL", "MEMORANDUM"]):
            return "document_header"
        elif any(keyword in content_upper for keyword in ["RISK", "WARNING", "CAUTION"]):
            return "risk_section"
        elif any(keyword in content_upper for keyword in ["INVESTMENT", "FUND", "PORTFOLIO"]):
            return "investment_section"
        elif any(keyword in content_upper for keyword in ["LEGAL", "REGULATORY", "COMPLIANCE"]):
            return "legal_section"
        elif re.search(r'\d+\.\d+%|\$\d+|USD|EUR', content):
            return "financial_data"
        else:
            return "general_content"
    
    def extract_to_chunks(self, file_path: str, output_dir: str = "C:/extrair", 
                         fund_identifier: str = None, map_id: int = None) -> Dict[str, Any]:
        """Extração principal para chunks contextuais com dados SQL."""
        
        file_path = Path(file_path)
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"EXTRAÇÃO PDF PARA CHUNKS CONTEXTUAIS COM SQL")
        print(f"Arquivo: {file_path.name}")
        print(f"Saída: {output_dir}")
        
        if map_id:
            print(f"MapID: {map_id}")
        elif fund_identifier:
            print(f"Identificador do fundo: {fund_identifier}")
        else:
            print(f"Identificador do fundo: Auto-detectar")
        
        try:
            doc = fitz.open(str(file_path))
            print(f"Total de páginas: {len(doc)}")
            
            # 1. Extrair metadados com integração SQL
            doc_metadata = self.extract_document_metadata(doc, fund_identifier, map_id)
            
            # 2. Extrair elementos de todas as páginas
            all_elements = []
            for page_num in range(len(doc)):
                page_elements = self.extract_page_elements(doc, page_num)
                all_elements.append(page_elements)
            
            # 3. Criar chunks contextuais
            content_chunks = self.create_content_chunks(all_elements)
            
            # 4. Estrutura final dos dados com informações SQL
            extracted_data = {
                "document_info": {
                    "filename": file_path.name,
                    "source_path": str(file_path),
                    "metadata": doc_metadata,
                    "extraction_config": {
                        "chunk_size": self.chunk_size,
                        "overlap": self.overlap,
                        "extraction_timestamp": str(datetime.datetime.now()),
                        "fund_identifier_used": fund_identifier,
                        "map_id_used": map_id
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
                "page_elements": all_elements
            }
            
            # 5. Salvar resultado
            output_file = output_dir / f"{file_path.stem}_chunks.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(extracted_data, f, indent=2, ensure_ascii=False)
            
            # 6. Mostrar resumo com dados SQL
            print(f"\nEXTRAÇÃO CONCLUÍDA!")
            print(f"Estatísticas:")
            print(f" - Chunks criados: {extracted_data['summary']['total_chunks']}")
            print(f" - Palavras totais: {extracted_data['summary']['total_words']:,}")
            print(f" - Imagens detectadas: {extracted_data['summary']['total_images']}")
            print(f" - Tabelas detectadas: {extracted_data['summary']['total_tables']}")
            
            # Mostrar informações SQL se disponível
            sql_info = doc_metadata.get("fund_database_info", {})
            if sql_info.get("sql_data_available", False):
                print(f"\nINFORMAÇÕES DO FUNDO (SQL):")
                print(f" - MapID: {sql_info.get('map_id', 'N/A')}")
                print(f" - Gestor: {sql_info.get('management_company', 'N/A')}")
                print(f" - Fundo: {sql_info.get('fund_name', 'N/A')}")
                print(f" - Nome curto: {sql_info.get('fund_short_name', 'N/A')}")
                print(f" - Asset class: {sql_info.get('asset_class_report', 'N/A')}")
                print(f" - Moeda: {sql_info.get('return_currency', 'N/A')}")
            else:
                print(f"\nInformações SQL não disponíveis")
                if sql_info.get("total_records_found", 0) > 1:
                    print(f"Sugestão: Use MapID específico para seleção precisa")
            
            print(f"\nArquivo salvo: {output_file}")
            
            doc.close()
            return extracted_data
            
        except Exception as e:
            print(f"Erro na extração: {e}")
            return None


def main():
    """Função principal com suporte a MapID e identificador de fundo."""
    print("Extrator PDF para Chunks Contextuais - Com Integração SQL")
    
    if len(sys.argv) < 2:
        print("\nCOMO USAR:")
        print(" python pdf_extractor.py <arquivo.pdf>")
        print(" python pdf_extractor.py <arquivo.pdf> <fund_identifier>")
        print(" python pdf_extractor.py <arquivo.pdf> --map-id <MapID>")
        print(" python pdf_extractor.py <arquivo.pdf> --map-id <MapID> <chunk_size>")
        print("\nExemplos:")
        print(" python pdf_extractor.py documento.pdf")
        print(" python pdf_extractor.py documento.pdf 'Pershing Square'")
        print(" python pdf_extractor.py documento.pdf --map-id 123")
        print(" python pdf_extractor.py documento.pdf --map-id 123 1500")
        return

    pdf_file = sys.argv[1]
    fund_identifier = None
    map_id = None
    chunk_size = 1000
    overlap = 200
    
    # Processar argumentos
    i = 2
    while i < len(sys.argv):
        if sys.argv[i] == "--map-id" and i + 1 < len(sys.argv):
            try:
                map_id = int(sys.argv[i + 1])
                i += 2
            except ValueError:
                print(f"Erro: MapID deve ser um número inteiro: {sys.argv[i + 1]}")
                return
        elif sys.argv[i].isdigit() and not map_id:  # chunk_size
            chunk_size = int(sys.argv[i])
            i += 1
        elif not fund_identifier and not map_id:  # fund_identifier
            fund_identifier = sys.argv[i]
            i += 1
        else:
            i += 1
    
    if not Path(pdf_file).exists():
        print(f"Arquivo não encontrado: {pdf_file}")
        return
    
    print(f"\nParametros:")
    print(f" - Arquivo: {pdf_file}")
    if map_id:
        print(f" - MapID: {map_id}")
    elif fund_identifier:
        print(f" - Identificador: {fund_identifier}")
    else:
        print(f" - Detecção automática ativada")
    print(f" - Chunk size: {chunk_size}")
    
    # Criar extrator e executar
    extractor = PDFToChunksExtractor(chunk_size=chunk_size, overlap=overlap)
    result = extractor.extract_to_chunks(pdf_file, fund_identifier=fund_identifier, map_id=map_id)
    
    if result:
        print(f"\nRESULTADO OTIMIZADO PARA LLM!")
        print(f"{result['summary']['total_chunks']} chunks prontos para análise")
        print(f"Cada chunk mantém contexto + informações do fundo do SQL")


if __name__ == "__main__":
    print("\nEXEMPLO PARA SEU ARQUIVO:")
    print(" python pdf_extractor.py C:\\extrair\\paginas.pdf --map-id 123")
    print(" python pdf_extractor.py C:\\extrair\\paginas.pdf 'Pershing Square'")
    print()
    main(),               # Só pipes
            r'^\s*\\\s*
        """Classifica o tipo de bloco baseado no conteúdo e formatação."""
        text_upper = text.upper()
        
        if any(keyword in text_upper for keyword in 
               ["CONFIDENTIAL", "MEMORANDUM", "FUND", "NOTICE", "REGULATORY"]):
            return "heading"
        
        if re.match(r'^\s*[-•▪▫]\s+', text) or re.match(r'^\s*\d+\.\s+', text):
            return "list_item"
        
        if len(re.findall(r'\b\d+\b', text)) > 3 and ('\t' in text or '  ' in text):
            return "table_data"
        
        if len(text) < 200 and (text.startswith('*') or text.startswith('Note:')):
            return "footnote"
        
        if len(text) > 100:
            return "paragraph"
        
        return "text_block"

    def _classify_image_type(self, img_data: Dict) -> str:
        """Classifica o tipo de imagem baseado nas características."""
        width = img_data.get("width", 0)
        height = img_data.get("height", 0)
        pixels = width * height
        aspect_ratio = width / height if height > 0 else 1
        
        if (pixels > 50000 and 
            0.5 <= aspect_ratio <= 3.0 and
            width > 300 and height > 200):
            return "chart"
        
        if pixels > 20000 and aspect_ratio > 2.5:
            return "diagram"
        
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
            
            if (re.search(r'\d+.*\d+.*\d+', line) or 
                '\t' in line or 
                len(re.findall(r'\s{3,}', line)) >= 2):
                
                current_table.append({
                    "line_number": i + 1,
                    "content": line
                })
            else:
                if len(current_table) >= 2:
                    tables.append({
                        "id": f"table_{len(tables) + 1}",
                        "start_line": current_table[0]["line_number"],
                        "end_line": current_table[-1]["line_number"],
                        "rows": current_table,
                        "row_count": len(current_table)
                    })
                current_table = []
        
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
        print(f"📦 Criando chunks de conteúdo...")
        
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
        
        for page_elements in all_elements:
            page_num = page_elements["page_number"]
            print(f"📄 Processando página {page_num} para chunks...")
            
            visual_context = ""
            if page_elements["images"]:
                visual_context += f"\n[PÁGINA {page_num} CONTÉM {len(page_elements['images'])} IMAGENS]"
                for img in page_elements["images"]:
                    if img.get("likely_type") in ["chart", "graph"]:
                        visual_context += f"\n[GRÁFICO: {img['dimensions']}]"
            
            if page_elements["tables"]:
                visual_context += f"\n[PÁGINA {page_num} CONTÉM {len(page_elements['tables'])} TABELAS]"
            
            for block in page_elements["structured_blocks"]:
                block_content = block["content"]
                block_type = block["type"]
                
                content_with_context = block_content
                if visual_context and block_type in ["heading", "paragraph"]:
                    content_with_context = block_content + visual_context
                    visual_context = ""
                
                if (len(current_chunk["content"]) + len(content_with_context) > self.chunk_size and 
                    len(current_chunk["content"]) > self.min_chunk_size):
                    
                    self._finalize_chunk(current_chunk, chunk_counter)
                    chunks.append(current_chunk)
                    
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
                
                if current_chunk["content"]:
                    current_chunk["content"] += "\n\n"
                current_chunk["content"] += content_with_context
                
                if page_num not in current_chunk["metadata"]["pages"]:
                    current_chunk["metadata"]["pages"].append(page_num)
                current_chunk["metadata"]["elements"].append(block["id"])
                if block_type not in current_chunk["metadata"]["content_types"]:
                    current_chunk["metadata"]["content_types"].append(block_type)
        
        if current_chunk["content"].strip():
            self._finalize_chunk(current_chunk, chunk_counter)
            chunks.append(current_chunk)
        
        print(f"✅ Criados {len(chunks)} chunks de conteúdo")
        
        for i, chunk in enumerate(chunks):
            chunk["context"]["chunk_position"] = f"{i + 1}/{len(chunks)}"
            if i > 0:
                chunk["context"]["previous_chunk_id"] = chunks[i - 1]["id"]
            if i < len(chunks) - 1:
                chunk["context"]["next_chunk_id"] = chunks[i + 1]["id"]
        
        return chunks

    def _finalize_chunk(self, chunk: Dict, chunk_id: int):
        """Finaliza um chunk calculando metadados."""
        chunk["id"] = f"chunk_{chunk_id}"
        chunk["metadata"]["word_count"] = len(chunk["content"].split())
        chunk["metadata"]["char_count"] = len(chunk["content"])

    def _get_overlap_content(self, content: str) -> str:
        """Obtém conteúdo de sobreposição do chunk anterior."""
        if len(content) <= self.overlap:
            return content
        
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
        
        summary = ' '.join(words[:10]) + " ... " + ' '.join(words[-10:])
        return summary

    def _get_section_context(self, content: str) -> str:
        """Identifica o contexto da seção atual."""
        content_upper = content.upper()
        
        if any(keyword in content_upper for keyword in ["CONFIDENTIAL", "MEMORANDUM"]):
            return "document_header"
        elif any(keyword in content_upper for keyword in ["RISK", "WARNING", "CAUTION"]):
            return "risk_section"
        elif any(keyword in content_upper for keyword in ["INVESTMENT", "FUND", "PORTFOLIO"]):
            return "investment_section"
        elif any(keyword in content_upper for keyword in ["LEGAL", "REGULATORY", "COMPLIANCE"]):
            return "legal_section"
        elif re.search(r'\d+\.\d+%|\$\d+|USD|EUR', content):
            return "financial_data"
        else:
            return "general_content"
    
    def extract_to_chunks(self, file_path: str, output_dir: str = "C:/extrair", 
                         fund_identifier: str = None, map_id: int = None) -> Dict[str, Any]:
        """Extração principal para chunks contextuais com dados SQL."""
        
        file_path = Path(file_path)
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"EXTRAÇÃO PDF PARA CHUNKS CONTEXTUAIS COM SQL")
        print(f"Arquivo: {file_path.name}")
        print(f"Saída: {output_dir}")
        
        if map_id:
            print(f"MapID: {map_id}")
        elif fund_identifier:
            print(f"Identificador do fundo: {fund_identifier}")
        else:
            print(f"Identificador do fundo: Auto-detectar")
        
        try:
            doc = fitz.open(str(file_path))
            print(f"Total de páginas: {len(doc)}")
            
            # 1. Extrair metadados com integração SQL
            doc_metadata = self.extract_document_metadata(doc, fund_identifier, map_id)
            
            # 2. Extrair elementos de todas as páginas
            all_elements = []
            for page_num in range(len(doc)):
                page_elements = self.extract_page_elements(doc, page_num)
                all_elements.append(page_elements)
            
            # 3. Criar chunks contextuais
            content_chunks = self.create_content_chunks(all_elements)
            
            # 4. Estrutura final dos dados com informações SQL
            extracted_data = {
                "document_info": {
                    "filename": file_path.name,
                    "source_path": str(file_path),
                    "metadata": doc_metadata,
                    "extraction_config": {
                        "chunk_size": self.chunk_size,
                        "overlap": self.overlap,
                        "extraction_timestamp": str(datetime.datetime.now()),
                        "fund_identifier_used": fund_identifier,
                        "map_id_used": map_id
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
                "page_elements": all_elements
            }
            
            # 5. Salvar resultado
            output_file = output_dir / f"{file_path.stem}_chunks.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(extracted_data, f, indent=2, ensure_ascii=False)
            
            # 6. Mostrar resumo com dados SQL
            print(f"\nEXTRAÇÃO CONCLUÍDA!")
            print(f"Estatísticas:")
            print(f" - Chunks criados: {extracted_data['summary']['total_chunks']}")
            print(f" - Palavras totais: {extracted_data['summary']['total_words']:,}")
            print(f" - Imagens detectadas: {extracted_data['summary']['total_images']}")
            print(f" - Tabelas detectadas: {extracted_data['summary']['total_tables']}")
            
            # Mostrar informações SQL se disponível
            sql_info = doc_metadata.get("fund_database_info", {})
            if sql_info.get("sql_data_available", False):
                print(f"\nINFORMAÇÕES DO FUNDO (SQL):")
                print(f" - MapID: {sql_info.get('map_id', 'N/A')}")
                print(f" - Gestor: {sql_info.get('management_company', 'N/A')}")
                print(f" - Fundo: {sql_info.get('fund_name', 'N/A')}")
                print(f" - Nome curto: {sql_info.get('fund_short_name', 'N/A')}")
                print(f" - Asset class: {sql_info.get('asset_class_report', 'N/A')}")
                print(f" - Moeda: {sql_info.get('return_currency', 'N/A')}")
            else:
                print(f"\nInformações SQL não disponíveis")
                if sql_info.get("total_records_found", 0) > 1:
                    print(f"Sugestão: Use MapID específico para seleção precisa")
            
            print(f"\nArquivo salvo: {output_file}")
            
            doc.close()
            return extracted_data
            
        except Exception as e:
            print(f"Erro na extração: {e}")
            return None


def main():
    """Função principal com suporte a MapID e identificador de fundo."""
    print("Extrator PDF para Chunks Contextuais - Com Integração SQL")
    
    if len(sys.argv) < 2:
        print("\nCOMO USAR:")
        print(" python pdf_extractor.py <arquivo.pdf>")
        print(" python pdf_extractor.py <arquivo.pdf> <fund_identifier>")
        print(" python pdf_extractor.py <arquivo.pdf> --map-id <MapID>")
        print(" python pdf_extractor.py <arquivo.pdf> --map-id <MapID> <chunk_size>")
        print("\nExemplos:")
        print(" python pdf_extractor.py documento.pdf")
        print(" python pdf_extractor.py documento.pdf 'Pershing Square'")
        print(" python pdf_extractor.py documento.pdf --map-id 123")
        print(" python pdf_extractor.py documento.pdf --map-id 123 1500")
        return

    pdf_file = sys.argv[1]
    fund_identifier = None
    map_id = None
    chunk_size = 1000
    overlap = 200
    
    # Processar argumentos
    i = 2
    while i < len(sys.argv):
        if sys.argv[i] == "--map-id" and i + 1 < len(sys.argv):
            try:
                map_id = int(sys.argv[i + 1])
                i += 2
            except ValueError:
                print(f"Erro: MapID deve ser um número inteiro: {sys.argv[i + 1]}")
                return
        elif sys.argv[i].isdigit() and not map_id:  # chunk_size
            chunk_size = int(sys.argv[i])
            i += 1
        elif not fund_identifier and not map_id:  # fund_identifier
            fund_identifier = sys.argv[i]
            i += 1
        else:
            i += 1
    
    if not Path(pdf_file).exists():
        print(f"Arquivo não encontrado: {pdf_file}")
        return
    
    print(f"\nParametros:")
    print(f" - Arquivo: {pdf_file}")
    if map_id:
        print(f" - MapID: {map_id}")
    elif fund_identifier:
        print(f" - Identificador: {fund_identifier}")
    else:
        print(f" - Detecção automática ativada")
    print(f" - Chunk size: {chunk_size}")
    
    # Criar extrator e executar
    extractor = PDFToChunksExtractor(chunk_size=chunk_size, overlap=overlap)
    result = extractor.extract_to_chunks(pdf_file, fund_identifier=fund_identifier, map_id=map_id)
    
    if result:
        print(f"\nRESULTADO OTIMIZADO PARA LLM!")
        print(f"{result['summary']['total_chunks']} chunks prontos para análise")
        print(f"Cada chunk mantém contexto + informações do fundo do SQL")


if __name__ == "__main__":
    print("\nEXEMPLO PARA SEU ARQUIVO:")
    print(" python pdf_extractor.py C:\\extrair\\paginas.pdf --map-id 123")
    print(" python pdf_extractor.py C:\\extrair\\paginas.pdf 'Pershing Square'")
    print()
    main(),               # Só barras
            r'^\s*\/\s*
        """Classifica o tipo de bloco baseado no conteúdo e formatação."""
        text_upper = text.upper()
        
        if any(keyword in text_upper for keyword in 
               ["CONFIDENTIAL", "MEMORANDUM", "FUND", "NOTICE", "REGULATORY"]):
            return "heading"
        
        if re.match(r'^\s*[-•▪▫]\s+', text) or re.match(r'^\s*\d+\.\s+', text):
            return "list_item"
        
        if len(re.findall(r'\b\d+\b', text)) > 3 and ('\t' in text or '  ' in text):
            return "table_data"
        
        if len(text) < 200 and (text.startswith('*') or text.startswith('Note:')):
            return "footnote"
        
        if len(text) > 100:
            return "paragraph"
        
        return "text_block"

    def _classify_image_type(self, img_data: Dict) -> str:
        """Classifica o tipo de imagem baseado nas características."""
        width = img_data.get("width", 0)
        height = img_data.get("height", 0)
        pixels = width * height
        aspect_ratio = width / height if height > 0 else 1
        
        if (pixels > 50000 and 
            0.5 <= aspect_ratio <= 3.0 and
            width > 300 and height > 200):
            return "chart"
        
        if pixels > 20000 and aspect_ratio > 2.5:
            return "diagram"
        
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
            
            if (re.search(r'\d+.*\d+.*\d+', line) or 
                '\t' in line or 
                len(re.findall(r'\s{3,}', line)) >= 2):
                
                current_table.append({
                    "line_number": i + 1,
                    "content": line
                })
            else:
                if len(current_table) >= 2:
                    tables.append({
                        "id": f"table_{len(tables) + 1}",
                        "start_line": current_table[0]["line_number"],
                        "end_line": current_table[-1]["line_number"],
                        "rows": current_table,
                        "row_count": len(current_table)
                    })
                current_table = []
        
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
        print(f"📦 Criando chunks de conteúdo...")
        
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
        
        for page_elements in all_elements:
            page_num = page_elements["page_number"]
            print(f"📄 Processando página {page_num} para chunks...")
            
            visual_context = ""
            if page_elements["images"]:
                visual_context += f"\n[PÁGINA {page_num} CONTÉM {len(page_elements['images'])} IMAGENS]"
                for img in page_elements["images"]:
                    if img.get("likely_type") in ["chart", "graph"]:
                        visual_context += f"\n[GRÁFICO: {img['dimensions']}]"
            
            if page_elements["tables"]:
                visual_context += f"\n[PÁGINA {page_num} CONTÉM {len(page_elements['tables'])} TABELAS]"
            
            for block in page_elements["structured_blocks"]:
                block_content = block["content"]
                block_type = block["type"]
                
                content_with_context = block_content
                if visual_context and block_type in ["heading", "paragraph"]:
                    content_with_context = block_content + visual_context
                    visual_context = ""
                
                if (len(current_chunk["content"]) + len(content_with_context) > self.chunk_size and 
                    len(current_chunk["content"]) > self.min_chunk_size):
                    
                    self._finalize_chunk(current_chunk, chunk_counter)
                    chunks.append(current_chunk)
                    
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
                
                if current_chunk["content"]:
                    current_chunk["content"] += "\n\n"
                current_chunk["content"] += content_with_context
                
                if page_num not in current_chunk["metadata"]["pages"]:
                    current_chunk["metadata"]["pages"].append(page_num)
                current_chunk["metadata"]["elements"].append(block["id"])
                if block_type not in current_chunk["metadata"]["content_types"]:
                    current_chunk["metadata"]["content_types"].append(block_type)
        
        if current_chunk["content"].strip():
            self._finalize_chunk(current_chunk, chunk_counter)
            chunks.append(current_chunk)
        
        print(f"✅ Criados {len(chunks)} chunks de conteúdo")
        
        for i, chunk in enumerate(chunks):
            chunk["context"]["chunk_position"] = f"{i + 1}/{len(chunks)}"
            if i > 0:
                chunk["context"]["previous_chunk_id"] = chunks[i - 1]["id"]
            if i < len(chunks) - 1:
                chunk["context"]["next_chunk_id"] = chunks[i + 1]["id"]
        
        return chunks

    def _finalize_chunk(self, chunk: Dict, chunk_id: int):
        """Finaliza um chunk calculando metadados."""
        chunk["id"] = f"chunk_{chunk_id}"
        chunk["metadata"]["word_count"] = len(chunk["content"].split())
        chunk["metadata"]["char_count"] = len(chunk["content"])

    def _get_overlap_content(self, content: str) -> str:
        """Obtém conteúdo de sobreposição do chunk anterior."""
        if len(content) <= self.overlap:
            return content
        
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
        
        summary = ' '.join(words[:10]) + " ... " + ' '.join(words[-10:])
        return summary

    def _get_section_context(self, content: str) -> str:
        """Identifica o contexto da seção atual."""
        content_upper = content.upper()
        
        if any(keyword in content_upper for keyword in ["CONFIDENTIAL", "MEMORANDUM"]):
            return "document_header"
        elif any(keyword in content_upper for keyword in ["RISK", "WARNING", "CAUTION"]):
            return "risk_section"
        elif any(keyword in content_upper for keyword in ["INVESTMENT", "FUND", "PORTFOLIO"]):
            return "investment_section"
        elif any(keyword in content_upper for keyword in ["LEGAL", "REGULATORY", "COMPLIANCE"]):
            return "legal_section"
        elif re.search(r'\d+\.\d+%|\$\d+|USD|EUR', content):
            return "financial_data"
        else:
            return "general_content"
    
    def extract_to_chunks(self, file_path: str, output_dir: str = "C:/extrair", 
                         fund_identifier: str = None, map_id: int = None) -> Dict[str, Any]:
        """Extração principal para chunks contextuais com dados SQL."""
        
        file_path = Path(file_path)
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"EXTRAÇÃO PDF PARA CHUNKS CONTEXTUAIS COM SQL")
        print(f"Arquivo: {file_path.name}")
        print(f"Saída: {output_dir}")
        
        if map_id:
            print(f"MapID: {map_id}")
        elif fund_identifier:
            print(f"Identificador do fundo: {fund_identifier}")
        else:
            print(f"Identificador do fundo: Auto-detectar")
        
        try:
            doc = fitz.open(str(file_path))
            print(f"Total de páginas: {len(doc)}")
            
            # 1. Extrair metadados com integração SQL
            doc_metadata = self.extract_document_metadata(doc, fund_identifier, map_id)
            
            # 2. Extrair elementos de todas as páginas
            all_elements = []
            for page_num in range(len(doc)):
                page_elements = self.extract_page_elements(doc, page_num)
                all_elements.append(page_elements)
            
            # 3. Criar chunks contextuais
            content_chunks = self.create_content_chunks(all_elements)
            
            # 4. Estrutura final dos dados com informações SQL
            extracted_data = {
                "document_info": {
                    "filename": file_path.name,
                    "source_path": str(file_path),
                    "metadata": doc_metadata,
                    "extraction_config": {
                        "chunk_size": self.chunk_size,
                        "overlap": self.overlap,
                        "extraction_timestamp": str(datetime.datetime.now()),
                        "fund_identifier_used": fund_identifier,
                        "map_id_used": map_id
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
                "page_elements": all_elements
            }
            
            # 5. Salvar resultado
            output_file = output_dir / f"{file_path.stem}_chunks.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(extracted_data, f, indent=2, ensure_ascii=False)
            
            # 6. Mostrar resumo com dados SQL
            print(f"\nEXTRAÇÃO CONCLUÍDA!")
            print(f"Estatísticas:")
            print(f" - Chunks criados: {extracted_data['summary']['total_chunks']}")
            print(f" - Palavras totais: {extracted_data['summary']['total_words']:,}")
            print(f" - Imagens detectadas: {extracted_data['summary']['total_images']}")
            print(f" - Tabelas detectadas: {extracted_data['summary']['total_tables']}")
            
            # Mostrar informações SQL se disponível
            sql_info = doc_metadata.get("fund_database_info", {})
            if sql_info.get("sql_data_available", False):
                print(f"\nINFORMAÇÕES DO FUNDO (SQL):")
                print(f" - MapID: {sql_info.get('map_id', 'N/A')}")
                print(f" - Gestor: {sql_info.get('management_company', 'N/A')}")
                print(f" - Fundo: {sql_info.get('fund_name', 'N/A')}")
                print(f" - Nome curto: {sql_info.get('fund_short_name', 'N/A')}")
                print(f" - Asset class: {sql_info.get('asset_class_report', 'N/A')}")
                print(f" - Moeda: {sql_info.get('return_currency', 'N/A')}")
            else:
                print(f"\nInformações SQL não disponíveis")
                if sql_info.get("total_records_found", 0) > 1:
                    print(f"Sugestão: Use MapID específico para seleção precisa")
            
            print(f"\nArquivo salvo: {output_file}")
            
            doc.close()
            return extracted_data
            
        except Exception as e:
            print(f"Erro na extração: {e}")
            return None


def main():
    """Função principal com suporte a MapID e identificador de fundo."""
    print("Extrator PDF para Chunks Contextuais - Com Integração SQL")
    
    if len(sys.argv) < 2:
        print("\nCOMO USAR:")
        print(" python pdf_extractor.py <arquivo.pdf>")
        print(" python pdf_extractor.py <arquivo.pdf> <fund_identifier>")
        print(" python pdf_extractor.py <arquivo.pdf> --map-id <MapID>")
        print(" python pdf_extractor.py <arquivo.pdf> --map-id <MapID> <chunk_size>")
        print("\nExemplos:")
        print(" python pdf_extractor.py documento.pdf")
        print(" python pdf_extractor.py documento.pdf 'Pershing Square'")
        print(" python pdf_extractor.py documento.pdf --map-id 123")
        print(" python pdf_extractor.py documento.pdf --map-id 123 1500")
        return

    pdf_file = sys.argv[1]
    fund_identifier = None
    map_id = None
    chunk_size = 1000
    overlap = 200
    
    # Processar argumentos
    i = 2
    while i < len(sys.argv):
        if sys.argv[i] == "--map-id" and i + 1 < len(sys.argv):
            try:
                map_id = int(sys.argv[i + 1])
                i += 2
            except ValueError:
                print(f"Erro: MapID deve ser um número inteiro: {sys.argv[i + 1]}")
                return
        elif sys.argv[i].isdigit() and not map_id:  # chunk_size
            chunk_size = int(sys.argv[i])
            i += 1
        elif not fund_identifier and not map_id:  # fund_identifier
            fund_identifier = sys.argv[i]
            i += 1
        else:
            i += 1
    
    if not Path(pdf_file).exists():
        print(f"Arquivo não encontrado: {pdf_file}")
        return
    
    print(f"\nParametros:")
    print(f" - Arquivo: {pdf_file}")
    if map_id:
        print(f" - MapID: {map_id}")
    elif fund_identifier:
        print(f" - Identificador: {fund_identifier}")
    else:
        print(f" - Detecção automática ativada")
    print(f" - Chunk size: {chunk_size}")
    
    # Criar extrator e executar
    extractor = PDFToChunksExtractor(chunk_size=chunk_size, overlap=overlap)
    result = extractor.extract_to_chunks(pdf_file, fund_identifier=fund_identifier, map_id=map_id)
    
    if result:
        print(f"\nRESULTADO OTIMIZADO PARA LLM!")
        print(f"{result['summary']['total_chunks']} chunks prontos para análise")
        print(f"Cada chunk mantém contexto + informações do fundo do SQL")


if __name__ == "__main__":
    print("\nEXEMPLO PARA SEU ARQUIVO:")
    print(" python pdf_extractor.py C:\\extrair\\paginas.pdf --map-id 123")
    print(" python pdf_extractor.py C:\\extrair\\paginas.pdf 'Pershing Square'")
    print()
    main(),               # Só barras
        ]
        
        if any(re.match(pattern, content_clean) for pattern in useless_patterns):
            return True
        
        # Ignorar se é principalmente pontuação
        punct_count = sum(1 for c in content_clean if c in '.,;:!?-_=*+~#()[]{}|\\/')
        if punct_count > len(content_clean) * 0.7:  # Mais de 70% pontuação
            return True
        
        return False
        """Classifica o tipo de bloco baseado no conteúdo e formatação."""
        text_upper = text.upper()
        
        if any(keyword in text_upper for keyword in 
               ["CONFIDENTIAL", "MEMORANDUM", "FUND", "NOTICE", "REGULATORY"]):
            return "heading"
        
        if re.match(r'^\s*[-•▪▫]\s+', text) or re.match(r'^\s*\d+\.\s+', text):
            return "list_item"
        
        if len(re.findall(r'\b\d+\b', text)) > 3 and ('\t' in text or '  ' in text):
            return "table_data"
        
        if len(text) < 200 and (text.startswith('*') or text.startswith('Note:')):
            return "footnote"
        
        if len(text) > 100:
            return "paragraph"
        
        return "text_block"

    def _classify_image_type(self, img_data: Dict) -> str:
        """Classifica o tipo de imagem baseado nas características."""
        width = img_data.get("width", 0)
        height = img_data.get("height", 0)
        pixels = width * height
        aspect_ratio = width / height if height > 0 else 1
        
        if (pixels > 50000 and 
            0.5 <= aspect_ratio <= 3.0 and
            width > 300 and height > 200):
            return "chart"
        
        if pixels > 20000 and aspect_ratio > 2.5:
            return "diagram"
        
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
            
            if (re.search(r'\d+.*\d+.*\d+', line) or 
                '\t' in line or 
                len(re.findall(r'\s{3,}', line)) >= 2):
                
                current_table.append({
                    "line_number": i + 1,
                    "content": line
                })
            else:
                if len(current_table) >= 2:
                    tables.append({
                        "id": f"table_{len(tables) + 1}",
                        "start_line": current_table[0]["line_number"],
                        "end_line": current_table[-1]["line_number"],
                        "rows": current_table,
                        "row_count": len(current_table)
                    })
                current_table = []
        
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
        print(f"📦 Criando chunks de conteúdo...")
        
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
        
        for page_elements in all_elements:
            page_num = page_elements["page_number"]
            print(f"📄 Processando página {page_num} para chunks...")
            
            visual_context = ""
            if page_elements["images"]:
                visual_context += f"\n[PÁGINA {page_num} CONTÉM {len(page_elements['images'])} IMAGENS]"
                for img in page_elements["images"]:
                    if img.get("likely_type") in ["chart", "graph"]:
                        visual_context += f"\n[GRÁFICO: {img['dimensions']}]"
            
            if page_elements["tables"]:
                visual_context += f"\n[PÁGINA {page_num} CONTÉM {len(page_elements['tables'])} TABELAS]"
            
            for block in page_elements["structured_blocks"]:
                block_content = block["content"]
                block_type = block["type"]
                
                content_with_context = block_content
                if visual_context and block_type in ["heading", "paragraph"]:
                    content_with_context = block_content + visual_context
                    visual_context = ""
                
                if (len(current_chunk["content"]) + len(content_with_context) > self.chunk_size and 
                    len(current_chunk["content"]) > self.min_chunk_size):
                    
                    self._finalize_chunk(current_chunk, chunk_counter)
                    chunks.append(current_chunk)
                    
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
                
                if current_chunk["content"]:
                    current_chunk["content"] += "\n\n"
                current_chunk["content"] += content_with_context
                
                if page_num not in current_chunk["metadata"]["pages"]:
                    current_chunk["metadata"]["pages"].append(page_num)
                current_chunk["metadata"]["elements"].append(block["id"])
                if block_type not in current_chunk["metadata"]["content_types"]:
                    current_chunk["metadata"]["content_types"].append(block_type)
        
        if current_chunk["content"].strip():
            self._finalize_chunk(current_chunk, chunk_counter)
            chunks.append(current_chunk)
        
        print(f"✅ Criados {len(chunks)} chunks de conteúdo")
        
        for i, chunk in enumerate(chunks):
            chunk["context"]["chunk_position"] = f"{i + 1}/{len(chunks)}"
            if i > 0:
                chunk["context"]["previous_chunk_id"] = chunks[i - 1]["id"]
            if i < len(chunks) - 1:
                chunk["context"]["next_chunk_id"] = chunks[i + 1]["id"]
        
        return chunks

    def _finalize_chunk(self, chunk: Dict, chunk_id: int):
        """Finaliza um chunk calculando metadados."""
        chunk["id"] = f"chunk_{chunk_id}"
        chunk["metadata"]["word_count"] = len(chunk["content"].split())
        chunk["metadata"]["char_count"] = len(chunk["content"])

    def _get_overlap_content(self, content: str) -> str:
        """Obtém conteúdo de sobreposição do chunk anterior."""
        if len(content) <= self.overlap:
            return content
        
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
        
        summary = ' '.join(words[:10]) + " ... " + ' '.join(words[-10:])
        return summary

    def _get_section_context(self, content: str) -> str:
        """Identifica o contexto da seção atual."""
        content_upper = content.upper()
        
        if any(keyword in content_upper for keyword in ["CONFIDENTIAL", "MEMORANDUM"]):
            return "document_header"
        elif any(keyword in content_upper for keyword in ["RISK", "WARNING", "CAUTION"]):
            return "risk_section"
        elif any(keyword in content_upper for keyword in ["INVESTMENT", "FUND", "PORTFOLIO"]):
            return "investment_section"
        elif any(keyword in content_upper for keyword in ["LEGAL", "REGULATORY", "COMPLIANCE"]):
            return "legal_section"
        elif re.search(r'\d+\.\d+%|\$\d+|USD|EUR', content):
            return "financial_data"
        else:
            return "general_content"
    
    def extract_to_chunks(self, file_path: str, output_dir: str = "C:/extrair", 
                         fund_identifier: str = None, map_id: int = None) -> Dict[str, Any]:
        """Extração principal para chunks contextuais com dados SQL."""
        
        file_path = Path(file_path)
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"EXTRAÇÃO PDF PARA CHUNKS CONTEXTUAIS COM SQL")
        print(f"Arquivo: {file_path.name}")
        print(f"Saída: {output_dir}")
        
        if map_id:
            print(f"MapID: {map_id}")
        elif fund_identifier:
            print(f"Identificador do fundo: {fund_identifier}")
        else:
            print(f"Identificador do fundo: Auto-detectar")
        
        try:
            doc = fitz.open(str(file_path))
            print(f"Total de páginas: {len(doc)}")
            
            # 1. Extrair metadados com integração SQL
            doc_metadata = self.extract_document_metadata(doc, fund_identifier, map_id)
            
            # 2. Extrair elementos de todas as páginas
            all_elements = []
            for page_num in range(len(doc)):
                page_elements = self.extract_page_elements(doc, page_num)
                all_elements.append(page_elements)
            
            # 3. Criar chunks contextuais
            content_chunks = self.create_content_chunks(all_elements)
            
            # 4. Estrutura final dos dados com informações SQL
            extracted_data = {
                "document_info": {
                    "filename": file_path.name,
                    "source_path": str(file_path),
                    "metadata": doc_metadata,
                    "extraction_config": {
                        "chunk_size": self.chunk_size,
                        "overlap": self.overlap,
                        "extraction_timestamp": str(datetime.datetime.now()),
                        "fund_identifier_used": fund_identifier,
                        "map_id_used": map_id
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
                "page_elements": all_elements
            }
            
            # 5. Salvar resultado
            output_file = output_dir / f"{file_path.stem}_chunks.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(extracted_data, f, indent=2, ensure_ascii=False)
            
            # 6. Mostrar resumo com dados SQL
            print(f"\nEXTRAÇÃO CONCLUÍDA!")
            print(f"Estatísticas:")
            print(f" - Chunks criados: {extracted_data['summary']['total_chunks']}")
            print(f" - Palavras totais: {extracted_data['summary']['total_words']:,}")
            print(f" - Imagens detectadas: {extracted_data['summary']['total_images']}")
            print(f" - Tabelas detectadas: {extracted_data['summary']['total_tables']}")
            
            # Mostrar informações SQL se disponível
            sql_info = doc_metadata.get("fund_database_info", {})
            if sql_info.get("sql_data_available", False):
                print(f"\nINFORMAÇÕES DO FUNDO (SQL):")
                print(f" - MapID: {sql_info.get('map_id', 'N/A')}")
                print(f" - Gestor: {sql_info.get('management_company', 'N/A')}")
                print(f" - Fundo: {sql_info.get('fund_name', 'N/A')}")
                print(f" - Nome curto: {sql_info.get('fund_short_name', 'N/A')}")
                print(f" - Asset class: {sql_info.get('asset_class_report', 'N/A')}")
                print(f" - Moeda: {sql_info.get('return_currency', 'N/A')}")
            else:
                print(f"\nInformações SQL não disponíveis")
                if sql_info.get("total_records_found", 0) > 1:
                    print(f"Sugestão: Use MapID específico para seleção precisa")
            
            print(f"\nArquivo salvo: {output_file}")
            
            doc.close()
            return extracted_data
            
        except Exception as e:
            print(f"Erro na extração: {e}")
            return None


def main():
    """Função principal com suporte a MapID e identificador de fundo."""
    print("Extrator PDF para Chunks Contextuais - Com Integração SQL")
    
    if len(sys.argv) < 2:
        print("\nCOMO USAR:")
        print(" python pdf_extractor.py <arquivo.pdf>")
        print(" python pdf_extractor.py <arquivo.pdf> <fund_identifier>")
        print(" python pdf_extractor.py <arquivo.pdf> --map-id <MapID>")
        print(" python pdf_extractor.py <arquivo.pdf> --map-id <MapID> <chunk_size>")
        print("\nExemplos:")
        print(" python pdf_extractor.py documento.pdf")
        print(" python pdf_extractor.py documento.pdf 'Pershing Square'")
        print(" python pdf_extractor.py documento.pdf --map-id 123")
        print(" python pdf_extractor.py documento.pdf --map-id 123 1500")
        return

    pdf_file = sys.argv[1]
    fund_identifier = None
    map_id = None
    chunk_size = 1000
    overlap = 200
    
    # Processar argumentos
    i = 2
    while i < len(sys.argv):
        if sys.argv[i] == "--map-id" and i + 1 < len(sys.argv):
            try:
                map_id = int(sys.argv[i + 1])
                i += 2
            except ValueError:
                print(f"Erro: MapID deve ser um número inteiro: {sys.argv[i + 1]}")
                return
        elif sys.argv[i].isdigit() and not map_id:  # chunk_size
            chunk_size = int(sys.argv[i])
            i += 1
        elif not fund_identifier and not map_id:  # fund_identifier
            fund_identifier = sys.argv[i]
            i += 1
        else:
            i += 1
    
    if not Path(pdf_file).exists():
        print(f"Arquivo não encontrado: {pdf_file}")
        return
    
    print(f"\nParametros:")
    print(f" - Arquivo: {pdf_file}")
    if map_id:
        print(f" - MapID: {map_id}")
    elif fund_identifier:
        print(f" - Identificador: {fund_identifier}")
    else:
        print(f" - Detecção automática ativada")
    print(f" - Chunk size: {chunk_size}")
    
    # Criar extrator e executar
    extractor = PDFToChunksExtractor(chunk_size=chunk_size, overlap=overlap)
    result = extractor.extract_to_chunks(pdf_file, fund_identifier=fund_identifier, map_id=map_id)
    
    if result:
        print(f"\nRESULTADO OTIMIZADO PARA LLM!")
        print(f"{result['summary']['total_chunks']} chunks prontos para análise")
        print(f"Cada chunk mantém contexto + informações do fundo do SQL")


if __name__ == "__main__":
    print("\nEXEMPLO PARA SEU ARQUIVO:")
    print(" python pdf_extractor.py C:\\extrair\\paginas.pdf --map-id 123")
    print(" python pdf_extractor.py C:\\extrair\\paginas.pdf 'Pershing Square'")
    print()
    main()