#!/usr/bin/env python3
"""
Extrator PDF Melhorado com Docling - Versão com Detecção Estrutural Avançada
Integra Docling para melhor identificação de headings, sections e estrutura hierárquica
"""

import json
import sys
import re
from pathlib import Path
import datetime
import pyodbc
import pandas as pd
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from collections import defaultdict
import hashlib

# Verificar dependências principais
try:
    import fitz
    print("PyMuPDF disponível")
except ImportError:
    print("Execute: pip install PyMuPDF")
    sys.exit(1)

# Verificar Docling
try:
    from docling.document_converter import DocumentConverter
    print("Docling disponível")
    DOCLING_AVAILABLE = True
except ImportError:
    print("Docling não disponível - usando fallback PyMuPDF")
    print("Para melhor detecção estrutural: pip install docling")
    DOCLING_AVAILABLE = False


@dataclass
class DocumentStructure:
    """Estrutura hierárquica do documento"""
    level: int
    title: str
    content: str
    start_page: int
    end_page: int
    parent_id: Optional[str] = None
    children_ids: List[str] = None
    section_type: str = "unknown"
    confidence: float = 0.0
    
    def __post_init__(self):
        if self.children_ids is None:
            self.children_ids = []


class EnhancedPDFExtractor:
    """Extrator PDF melhorado com detecção estrutural via Docling."""

    def __init__(self, chunk_size: int = 1000, overlap: int = 200):
        self.chunk_size = chunk_size
        self.overlap = overlap
        self.min_chunk_size = 100
        self.document_structures: List[DocumentStructure] = []
        
        # Configurar Docling se disponível
        if DOCLING_AVAILABLE:
            self.docling_converter = DocumentConverter()
        
    def extract_with_docling(self, file_path: Path) -> Optional[Dict]:
        """Extrai estrutura do documento usando Docling."""
        if not DOCLING_AVAILABLE:
            return None
            
        try:
            print("Extraindo estrutura com Docling...")
            
            # Configurar pipeline específico para PDFs
            pipeline_options = PdfPipelineOptions()
            pipeline_options.do_ocr = False  # Desabilitar OCR por padrão (mais rápido)
            pipeline_options.do_table_structure = True  # Detectar estrutura de tabelas
            
            # Converter documento
            result = self.docling_converter.convert(
                str(file_path), 
                pipeline_options=pipeline_options
            )
            
            # Extrair dados estruturais
            doc_data = result.document
            
            docling_result = {
                "title": getattr(doc_data, 'title', None) or file_path.stem,
                "authors": getattr(doc_data, 'authors', []),
                "creation_date": getattr(doc_data, 'creation_date', None),
                "page_count": len(doc_data.pages) if hasattr(doc_data, 'pages') else 0,
                "sections": [],
                "tables": [],
                "figures": [],
                "text_blocks": []
            }
            
            # Processar elementos estruturais
            if hasattr(doc_data, 'body') and doc_data.body:
                for element in doc_data.body.children:
                    element_data = self._process_docling_element(element)
                    if element_data:
                        docling_result["text_blocks"].append(element_data)
            
            # Detectar estrutura hierárquica
            self._build_document_hierarchy(docling_result["text_blocks"])
            
            print(f"Docling: {len(docling_result['text_blocks'])} elementos estruturais detectados")
            print(f"Hierarquia: {len(self.document_structures)} seções identificadas")
            
            return docling_result
            
        except Exception as e:
            print(f"Erro no Docling (continuando com PyMuPDF): {e}")
            return None
    
    def _process_docling_element(self, element) -> Optional[Dict]:
        """Processa um elemento individual do Docling."""
        try:
            element_type = type(element).__name__.lower()
            
            # Mapear tipos do Docling para nossos tipos
            type_mapping = {
                'title': 'heading',
                'section-header': 'heading', 
                'paragraph': 'paragraph',
                'list': 'list',
                'table': 'table',
                'figure': 'figure',
                'caption': 'caption'
            }
            
            mapped_type = type_mapping.get(element_type, 'text_block')
            
            # Extrair texto do elemento
            text_content = ""
            if hasattr(element, 'text'):
                text_content = element.text
            elif hasattr(element, 'content'):
                text_content = str(element.content)
            
            if not text_content or len(text_content.strip()) < 3:
                return None
            
            # Determinar nível hierárquico para headings
            level = 0
            if mapped_type == 'heading':
                level = self._determine_heading_level(text_content, element)
            
            # Extrair informações de posição se disponível
            page_number = getattr(element, 'page_number', 0)
            bbox = getattr(element, 'bbox', [0, 0, 0, 0])
            
            return {
                "id": f"docling_{element_type}_{hash(text_content) % 10000}",
                "type": mapped_type,
                "content": text_content.strip(),
                "level": level,
                "page": page_number,
                "bbox": bbox,
                "confidence": 0.9,  # Alta confiança para elementos do Docling
                "source": "docling"
            }
            
        except Exception as e:
            print(f"Erro ao processar elemento Docling: {e}")
            return None
    
    def _determine_heading_level(self, text: str, element) -> int:
        """Determina o nível hierárquico de um heading."""
        
        # Tentar obter nível do próprio elemento
        if hasattr(element, 'level'):
            return min(element.level, 6)
        
        # Análise heurística do texto
        text_clean = text.strip()
        
        # Padrões de nível 1 (títulos principais)
        level1_patterns = [
            r'^(EXECUTIVE\s+SUMMARY|INVESTMENT\s+OVERVIEW|FUND\s+OVERVIEW)$',
            r'^(CONTENTS?|TABLE\s+OF\s+CONTENTS?)$',
            r'^[IVX]{1,4}\.\s+',  # Numeração romana
            r'^[A-Z\s]{3,20}$'  # Texto todo em maiúsculas, tamanho médio
        ]
        
        # Padrões de nível 2
        level2_patterns = [
            r'^\d+\.\s+[A-Z]',  # "1. Investment Strategy"
            r'^[A-Z][a-z]+\s+[A-Z][a-z]+$',  # "Investment Strategy"
        ]
        
        # Padrões de nível 3+
        level3_patterns = [
            r'^\d+\.\d+\s+',  # "1.1 Subsection"
            r'^[a-z]\)\s+',   # "a) Point"
        ]
        
        for pattern in level1_patterns:
            if re.match(pattern, text_clean, re.IGNORECASE):
                return 1
                
        for pattern in level2_patterns:
            if re.match(pattern, text_clean):
                return 2
                
        for pattern in level3_patterns:
            if re.match(pattern, text_clean):
                return 3
        
        # Análise por comprimento e formato
        if len(text_clean.split()) <= 4 and text_clean.isupper():
            return 2
        elif len(text_clean.split()) <= 8:
            return 3
        else:
            return 4
    
    def _build_document_hierarchy(self, elements: List[Dict]):
        """Constrói hierarquia do documento baseada nos elementos."""
        current_hierarchy = []
        
        for element in elements:
            if element["type"] == "heading":
                level = element["level"]
                
                # Encontrar parent correto baseado no nível
                parent_id = None
                if current_hierarchy:
                    # Buscar o último heading de nível menor
                    for i in range(len(current_hierarchy) - 1, -1, -1):
                        if current_hierarchy[i].level < level:
                            parent_id = f"section_{i}"
                            break
                
                # Criar estrutura da seção
                section = DocumentStructure(
                    level=level,
                    title=element["content"],
                    content="",  # Será preenchido depois
                    start_page=element["page"],
                    end_page=element["page"],
                    parent_id=parent_id,
                    section_type=self._classify_section_type(element["content"]),
                    confidence=element["confidence"]
                )
                
                # Ajustar hierarquia atual
                current_hierarchy = [s for s in current_hierarchy if s.level < level]
                current_hierarchy.append(section)
                
                self.document_structures.append(section)
    
    def _classify_section_type(self, title: str) -> str:
        """Classifica o tipo de seção baseado no título."""
        title_upper = title.upper()
        
        # Mapeamento de padrões para tipos
        section_patterns = {
            "executive_summary": [
                r"EXECUTIVE\s+SUMMARY",
                r"SUMMARY",
                r"OVERVIEW"
            ],
            "investment_strategy": [
                r"INVESTMENT\s+(STRATEGY|APPROACH|OBJECTIVE)",
                r"STRATEGY",
                r"APPROACH",
                r"METHODOLOGY"
            ],
            "fund_information": [
                r"FUND\s+(INFORMATION|DETAILS|FACTS)",
                r"SHARE\s+CLASS",
                r"FUND\s+OVERVIEW"
            ],
            "performance": [
                r"PERFORMANCE",
                r"RETURNS?",
                r"TRACK\s+RECORD"
            ],
            "risk_factors": [
                r"RISK\s+(FACTORS?|WARNING|DISCLOSURE)",
                r"IMPORTANT\s+NOTICES?",
                r"WARNING"
            ],
            "legal": [
                r"LEGAL",
                r"REGULATORY",
                r"DISCLAIMER",
                r"TERMS\s+AND\s+CONDITIONS"
            ],
            "fees": [
                r"FEE",
                r"COST",
                r"EXPENSE",
                r"CHARGE"
            ],
            "contact": [
                r"CONTACT",
                r"ADDRESS",
                r"ADMINISTRATOR"
            ]
        }
        
        for section_type, patterns in section_patterns.items():
            for pattern in patterns:
                if re.search(pattern, title_upper):
                    return section_type
        
        return "general"
    
    @staticmethod
    def get_data_from_sql(query):
        """Função para puxar dados do SQL (mantida da versão original)."""
        try:
            from sqlalchemy import create_engine
            import urllib
            
            params = urllib.parse.quote_plus(
                'Driver={SQL Server};'
                'Server=sql.msquare.local;'
                'Database=M_SQUARE_Prod;'
                'Trusted_Connection=yes;'
            )
            engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
            return pd.read_sql(query, engine)
            
        except ImportError:
            try:
                cnxn = pyodbc.connect('Driver={SQL Server};'
                                    'Server=sql.msquare.local;'
                                    'Database=M_SQUARE_Prod;'
                                    'Trusted_Connection=yes;')
                return pd.read_sql(query, cnxn)
            except Exception as e:
                print(f"Erro ao conectar com SQL: {e}")
                return None
        except Exception as e:
            print(f"Erro ao conectar com SQL: {e}")
            return None
    
    @staticmethod
    def get_fund_info_from_sql(fund_identifier: str = None, map_id: int = None) -> Dict[str, Any]:
        """Busca informações do fundo (mantida da versão original)."""
        query = """
        SELECT MapID, MgtCompany, FundName, FundShortName, AssetClassReport, Return_Currency 
        FROM Core.Tbl_Dim_FundInformationMapping
        """
        
        if map_id:
            query += f" WHERE MapID = {map_id}"
            print(f"Buscando por MapID: {map_id}")
        elif fund_identifier:
            query += f"""
            WHERE FundName LIKE '%{fund_identifier}%' 
               OR FundShortName LIKE '%{fund_identifier}%'
               OR MgtCompany LIKE '%{fund_identifier}%'
            """
            print(f"Buscando por identificador: {fund_identifier}")
        
        df = EnhancedPDFExtractor.get_data_from_sql(query)
        
        if df is None or df.empty:
            return {
                "sql_data_available": False,
                "error": "No data found in database"
            }
        
        selected_row = df.iloc[0]
        return {
            "sql_data_available": True,
            "map_id": int(selected_row['MapID']),
            "management_company": selected_row['MgtCompany'],
            "fund_name": selected_row['FundName'],
            "fund_short_name": selected_row['FundShortName'],
            "asset_class_report": selected_row['AssetClassReport'],
            "return_currency": selected_row['Return_Currency'],
        }
    
    def create_enhanced_metadata(self, doc, file_path: Path, docling_data: Dict = None,
                               fund_identifier: str = None, map_id: int = None) -> Dict[str, Any]:
        """Cria metadados melhorados com informações estruturais."""
        
        # Metadados básicos
        try:
            pdf_metadata = doc.metadata
        except:
            pdf_metadata = {}
        
        # Buscar informações do fundo
        sql_info = self.get_fund_info_from_sql(fund_identifier, map_id)
        
        # Análise estrutural
        structure_analysis = {
            "total_sections": len(self.document_structures),
            "hierarchy_levels": len(set(s.level for s in self.document_structures)),
            "section_types": list(set(s.section_type for s in self.document_structures)),
            "sections_by_level": {
                level: len([s for s in self.document_structures if s.level == level])
                for level in range(1, 7)
            }
        }
        
        metadata = {
            "file_info": {
                "filename": file_path.name,
                "source_path": str(file_path),
                "total_pages": len(doc),
                "creation_date": pdf_metadata.get("creationDate", ""),
                "modification_date": pdf_metadata.get("modDate", "")
            },
            "fund_info": sql_info,
            "document_structure": structure_analysis,
            "extraction_config": {
                "chunk_size": self.chunk_size,
                "overlap": self.overlap,
                "docling_used": docling_data is not None,
                "extraction_timestamp": datetime.datetime.now().isoformat(),
            }
        }
        
        # Adicionar dados do Docling se disponível
        if docling_data:
            metadata["docling_analysis"] = {
                "detected_title": docling_data.get("title"),
                "authors": docling_data.get("authors", []),
                "structural_elements": len(docling_data.get("text_blocks", [])),
                "tables_detected": len(docling_data.get("tables", [])),
                "figures_detected": len(docling_data.get("figures", []))
            }
        
        return metadata
    
    def create_structured_chunks(self, docling_data: Dict = None, all_elements: List[Dict] = None) -> List[Dict]:
        """Cria chunks respeitando a estrutura hierárquica do documento."""
        print("Criando chunks estruturais...")
        
        chunks = []
        
        # Se temos estrutura do Docling, usar ela
        if docling_data and docling_data.get("text_blocks"):
            chunks = self._create_chunks_from_docling(docling_data)
        elif all_elements:
            chunks = self._create_chunks_from_pymupdf(all_elements)
        
        # Enriquecer chunks com contexto estrutural
        self._enrich_chunks_with_structure(chunks)
        
        print(f"Criados {len(chunks)} chunks estruturais")
        return chunks
    
    def _create_chunks_from_docling(self, docling_data: Dict) -> List[Dict]:
        """Cria chunks baseados na estrutura do Docling."""
        chunks = []
        current_chunk_content = ""
        current_section = None
        current_elements = []
        
        for element in docling_data["text_blocks"]:
            element_content = element["content"]
            
            # Se é um heading, pode iniciar nova seção
            if element["type"] == "heading":
                # Finalizar chunk anterior se existir
                if current_chunk_content.strip():
                    chunk = self._create_chunk_from_content(
                        current_chunk_content, 
                        current_section, 
                        current_elements,
                        len(chunks) + 1
                    )
                    chunks.append(chunk)
                    current_chunk_content = ""
                    current_elements = []
                
                current_section = element
            
            # Verificar se adicionar este elemento excederia o tamanho do chunk
            if (len(current_chunk_content) + len(element_content) > self.chunk_size and 
                len(current_chunk_content) > self.min_chunk_size):
                
                # Finalizar chunk atual
                chunk = self._create_chunk_from_content(
                    current_chunk_content, 
                    current_section, 
                    current_elements,
                    len(chunks) + 1
                )
                chunks.append(chunk)
                
                # Iniciar novo chunk com overlap
                overlap_content = self._get_overlap_content(current_chunk_content)
                current_chunk_content = overlap_content
                current_elements = []
            
            # Adicionar elemento ao chunk atual
            if current_chunk_content:
                current_chunk_content += "\n\n"
            current_chunk_content += element_content
            current_elements.append(element)
        
        # Finalizar último chunk
        if current_chunk_content.strip():
            chunk = self._create_chunk_from_content(
                current_chunk_content, 
                current_section, 
                current_elements,
                len(chunks) + 1
            )
            chunks.append(chunk)
        
        return chunks
    
    def _create_chunk_from_content(self, content: str, section: Dict = None, 
                                 elements: List[Dict] = None, chunk_id: int = 1) -> Dict:
        """Cria um chunk com metadados estruturais."""
        
        if elements is None:
            elements = []
        
        # Determinar contexto da seção
        section_context = "general_content"
        section_title = ""
        section_level = 0
        
        if section:
            section_context = self._classify_section_type(section["content"])
            section_title = section["content"]
            section_level = section.get("level", 0)
        
        return {
            "id": f"chunk_{chunk_id}",
            "content": content.strip(),
            "metadata": {
                "word_count": len(content.split()),
                "char_count": len(content),
                "pages": list(set(el.get("page", 1) for el in elements if el.get("page"))),
                "elements": [el.get("id", f"element_{i}") for i, el in enumerate(elements)],
                "content_types": list(set(el.get("type", "text") for el in elements)),
                "structural_confidence": sum(el.get("confidence", 0.5) for el in elements) / len(elements) if elements else 0.5
            },
            "structure": {
                "section_title": section_title,
                "section_type": section_context,
                "section_level": section_level,
                "position_in_section": self._determine_position_in_section(elements),
                "hierarchy_path": self._get_hierarchy_path(section_title)
            },
            "context": {
                "document_position": f"~{(chunk_id / 100) * 100:.0f}% do documento",
                "previous_chunk_summary": "",
                "section_context": {
                    "type": section_context,
                    "confidence": 0.9 if section else 0.5,
                    "is_reliable": True if section else False
                }
            }
        }
    
    def _create_chunks_from_pymupdf(self, all_elements: List[Dict]) -> List[Dict]:
        """Fallback: criar chunks usando dados do PyMuPDF."""
        chunks = []
        current_chunk = ""
        current_metadata = {
            "pages": [],
            "elements": [],
            "content_types": []
        }
        
        for page_elements in all_elements:
            for block in page_elements.get("structured_blocks", []):
                block_content = block["content"]
                
                if (len(current_chunk) + len(block_content) > self.chunk_size and 
                    len(current_chunk) > self.min_chunk_size):
                    
                    # Finalizar chunk atual
                    chunk = {
                        "id": f"chunk_{len(chunks) + 1}",
                        "content": current_chunk.strip(),
                        "metadata": {
                            "word_count": len(current_chunk.split()),
                            "char_count": len(current_chunk),
                            "pages": current_metadata["pages"],
                            "elements": current_metadata["elements"],
                            "content_types": current_metadata["content_types"],
                            "structural_confidence": 0.6  # Menor confiança sem Docling
                        },
                        "structure": {
                            "section_title": "",
                            "section_type": "unknown",
                            "section_level": 0,
                            "position_in_section": "unknown",
                            "hierarchy_path": []
                        },
                        "context": {
                            "document_position": f"~{len(chunks) * 10:.0f}% do documento",
                            "section_context": {
                                "type": "general_content",
                                "confidence": 0.5,
                                "is_reliable": False
                            }
                        }
                    }
                    chunks.append(chunk)
                    
                    # Reiniciar para próximo chunk
                    overlap_content = self._get_overlap_content(current_chunk)
                    current_chunk = overlap_content
                    current_metadata = {
                        "pages": [page_elements["page_number"]],
                        "elements": [block["id"]],
                        "content_types": [block["type"]]
                    }
                
                # Adicionar conteúdo atual
                if current_chunk:
                    current_chunk += "\n\n"
                current_chunk += block_content
                
                if page_elements["page_number"] not in current_metadata["pages"]:
                    current_metadata["pages"].append(page_elements["page_number"])
                current_metadata["elements"].append(block["id"])
                if block["type"] not in current_metadata["content_types"]:
                    current_metadata["content_types"].append(block["type"])
        
        # Finalizar último chunk
        if current_chunk.strip():
            chunk = {
                "id": f"chunk_{len(chunks) + 1}",
                "content": current_chunk.strip(),
                "metadata": current_metadata,
                "structure": {"section_title": "", "section_type": "unknown"},
                "context": {"section_context": {"type": "general_content", "confidence": 0.5}}
            }
            chunks.append(chunk)
        
        return chunks
    
    def _enrich_chunks_with_structure(self, chunks: List[Dict]):
        """Enriquece chunks com informações estruturais adicionais."""
        
        for i, chunk in enumerate(chunks):
            # Adicionar referências aos chunks vizinhos
            chunk["context"]["chunk_position"] = f"{i + 1}/{len(chunks)}"
            
            if i > 0:
                chunk["context"]["previous_chunk_id"] = chunks[i - 1]["id"]
                chunk["context"]["previous_chunk_summary"] = self._create_chunk_summary(
                    chunks[i - 1]["content"]
                )
            
            if i < len(chunks) - 1:
                chunk["context"]["next_chunk_id"] = chunks[i + 1]["id"]
            
            # Melhorar informações de seção baseadas na estrutura hierárquica
            section_info = self._find_relevant_section_for_chunk(chunk, i, len(chunks))
            if section_info:
                chunk["structure"].update(section_info)
    
    def _find_relevant_section_for_chunk(self, chunk: Dict, chunk_index: int, total_chunks: int) -> Dict:
        """Encontra a seção mais relevante para o chunk baseado na estrutura hierárquica."""
        
        if not self.document_structures:
            return {}
        
        # Estimar posição do chunk no documento
        estimated_position = chunk_index / total_chunks
        
        # Encontrar seção mais provável baseada na posição
        relevant_sections = []
        for section in self.document_structures:
            if section.level <= 2:  # Considerar apenas seções principais
                relevant_sections.append(section)
        
        if not relevant_sections:
            return {}
        
        # Selecionar seção baseada na posição estimada
        section_index = min(int(estimated_position * len(relevant_sections)), 
                          len(relevant_sections) - 1)
        selected_section = relevant_sections[section_index]
        
        return {
            "section_title": selected_section.title,
            "section_type": selected_section.section_type,
            "section_level": selected_section.level,
            "hierarchy_path": self._get_hierarchy_path(selected_section.title)
        }
    
    def _determine_position_in_section(self, elements: List[Dict]) -> str:
        """Determina a posição dos elementos dentro da seção."""
        if not elements:
            return "unknown"
        
        has_heading = any(el.get("type") == "heading" for el in elements)
        has_paragraph = any(el.get("type") == "paragraph" for el in elements)
        
        if has_heading and has_paragraph:
            return "section_start"
        elif has_heading:
            return "section_header"
        elif has_paragraph:
            return "section_body"
        else:
            return "section_content"
    
    def _get_hierarchy_path(self, section_title: str) -> List[str]:
        """Constrói o caminho hierárquico da seção."""
        path = []
        
        for structure in self.document_structures:
            if structure.title == section_title:
                # Construir caminho da raiz até esta seção
                current = structure
                temp_path = [current.title]
                
                while current.parent_id:
                    parent = next((s for s in self.document_structures 
                                 if f"section_{self.document_structures.index(s)}" == current.parent_id), None)
                    if parent:
                        temp_path.insert(0, parent.title)
                        current = parent
                    else:
                        break
                
                return temp_path
        
        return [section_title] if section_title else []
    
    def _finalize_chunk(self, chunk: Dict, chunk_id: int):
        """Finaliza um chunk calculando metadados finais."""
        chunk["id"] = f"chunk_{chunk_id}"
        if "word_count" not in chunk["metadata"]:
            chunk["metadata"]["word_count"] = len(chunk["content"].split())
        if "char_count" not in chunk["metadata"]:
            chunk["metadata"]["char_count"] = len(chunk["content"])
    
    def _get_overlap_content(self, content: str) -> str:
        """Obtém conteúdo de sobreposição (mantido da versão original)."""
        if len(content) <= self.overlap:
            return content
        
        overlap_start = len(content) - self.overlap
        sentences = re.split(r'[.!?]\s+', content[overlap_start:])
        
        if len(sentences) > 1:
            return '. '.join(sentences[1:]) + '.'
        else:
            return content[-self.overlap:]
    
    def _create_chunk_summary(self, content: str) -> str:
        """Cria resumo do chunk (mantido da versão original)."""
        words = content.split()
        if len(words) <= 20:
            return content
        
        return ' '.join(words[:10]) + " ... " + ' '.join(words[-10:])
    
    def extract_to_enhanced_chunks(self, file_path: str, output_dir: str = "C:/extrair",
                                 fund_identifier: str = None, map_id: int = None) -> Dict[str, Any]:
        """Extração principal melhorada com estrutura hierárquica."""
        
        file_path = Path(file_path)
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"EXTRAÇÃO PDF MELHORADA COM DOCLING E ESTRUTURA HIERÁRQUICA")
        print(f"Arquivo: {file_path.name}")
        print(f"Docling disponível: {DOCLING_AVAILABLE}")
        
        try:
            # 1. Extrair estrutura com Docling (se disponível)
            docling_data = None
            if DOCLING_AVAILABLE:
                docling_data = self.extract_with_docling(file_path)
            
            # 2. Fallback com PyMuPDF para dados detalhados
            doc = fitz.open(str(file_path))
            print(f"Total de páginas: {len(doc)}")
            
            # Extrair elementos detalhados por página (para contexto)
            all_elements = []
            for page_num in range(len(doc)):
                page = doc[page_num]
                page_data = {
                    "page_number": page_num + 1,
                    "text_content": page.get_text(),
                    "structured_blocks": self._extract_pymupdf_blocks(page, page_num)
                }
                all_elements.append(page_data)
            
            # 3. Criar metadados melhorados
            metadata = self.create_enhanced_metadata(
                doc, file_path, docling_data, fund_identifier, map_id
            )
            
            # 4. Criar chunks estruturais
            chunks = self.create_structured_chunks(docling_data, all_elements)
            
            # 5. Estrutura final melhorada
            extracted_data = {
                "metadata": metadata,
                "document_structure": [asdict(s) for s in self.document_structures],
                "content_chunks": chunks,
                "statistics": {
                    "total_chunks": len(chunks),
                    "total_pages": len(doc),
                    "total_words": sum(chunk["metadata"]["word_count"] for chunk in chunks),
                    "average_chunk_size": round(
                        sum(chunk["metadata"]["word_count"] for chunk in chunks) / len(chunks), 1
                    ) if chunks else 0,
                    "structural_sections": len(self.document_structures),
                    "hierarchy_levels": len(set(s.level for s in self.document_structures)),
                    "high_confidence_chunks": len([
                        c for c in chunks 
                        if c["metadata"]["structural_confidence"] > 0.7
                    ]),
                    "section_types_found": list(set(
                        c["structure"]["section_type"] for c in chunks
                        if c["structure"]["section_type"] != "unknown"
                    ))
                }
            }
            
            # 6. Salvar resultado
            output_file = output_dir / f"{file_path.stem}_enhanced_chunks.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(extracted_data, f, indent=2, ensure_ascii=False)
            
            # 7. Mostrar resumo detalhado
            self._print_enhanced_summary(extracted_data, output_file)
            
            doc.close()
            return extracted_data
            
        except Exception as e:
            print(f"Erro na extração melhorada: {e}")
            return None
    
    def _extract_pymupdf_blocks(self, page, page_num: int) -> List[Dict]:
        """Extrai blocos usando PyMuPDF como fallback."""
        blocks = []
        
        try:
            text_dict = page.get_text("dict")
            for i, block in enumerate(text_dict.get("blocks", [])):
                if "lines" in block:
                    block_text = ""
                    for line in block["lines"]:
                        for span in line.get("spans", []):
                            text = span.get("text", "").strip()
                            if text:
                                block_text += text + " "
                    
                    if block_text.strip():
                        blocks.append({
                            "id": f"page_{page_num + 1}_block_{i + 1}",
                            "type": "text_block",
                            "content": block_text.strip(),
                            "bbox": block.get("bbox", [])
                        })
        except Exception as e:
            print(f"Erro ao extrair blocos PyMuPDF: {e}")
        
        return blocks
    
    def _print_enhanced_summary(self, extracted_data: Dict, output_file: Path):
        """Imprime resumo melhorado dos resultados."""
        stats = extracted_data["statistics"]
        metadata = extracted_data["metadata"]
        
        print(f"\n{'='*60}")
        print(f"EXTRAÇÃO CONCLUÍDA COM SUCESSO!")
        print(f"{'='*60}")
        
        print(f"\n📊 ESTATÍSTICAS GERAIS:")
        print(f" - Chunks criados: {stats['total_chunks']}")
        print(f" - Páginas processadas: {stats['total_pages']}")
        print(f" - Palavras totais: {stats['total_words']:,}")
        print(f" - Tamanho médio por chunk: {stats['average_chunk_size']} palavras")
        
        print(f"\n🏗️ ESTRUTURA HIERÁRQUICA:")
        print(f" - Seções identificadas: {stats['structural_sections']}")
        print(f" - Níveis hierárquicos: {stats['hierarchy_levels']}")
        print(f" - Chunks alta confiança: {stats['high_confidence_chunks']}/{stats['total_chunks']}")
        
        if stats['section_types_found']:
            print(f" - Tipos de seção: {', '.join(stats['section_types_found'])}")
        
        # Mostrar informações do Docling se disponível
        if metadata["extraction_config"]["docling_used"]:
            docling_info = metadata.get("docling_analysis", {})
            print(f"\n🔍 ANÁLISE DOCLING:")
            print(f" - Título detectado: {docling_info.get('detected_title', 'N/A')}")
            print(f" - Elementos estruturais: {docling_info.get('structural_elements', 0)}")
            print(f" - Tabelas detectadas: {docling_info.get('tables_detected', 0)}")
            print(f" - Figuras detectadas: {docling_info.get('figures_detected', 0)}")
        
        # Informações do fundo
        fund_info = metadata["fund_info"]
        if fund_info.get("sql_data_available"):
            print(f"\n💼 INFORMAÇÕES DO FUNDO:")
            print(f" - MapID: {fund_info.get('map_id', 'N/A')}")
            print(f" - Gestor: {fund_info.get('management_company', 'N/A')}")
            print(f" - Fundo: {fund_info.get('fund_name', 'N/A')}")
        
        print(f"\n💾 Arquivo salvo: {output_file}")
        print(f"{'='*60}")


def main():
    """Função principal melhorada."""
    print("Extrator PDF Melhorado com Docling - Estrutura Hierárquica Avançada")
    
    if len(sys.argv) < 2:
        print("\nCOMO USAR:")
        print(" python enhanced_pdf_extractor.py <arquivo.pdf>")
        print(" python enhanced_pdf_extractor.py <arquivo.pdf> <fund_identifier>")
        print(" python enhanced_pdf_extractor.py <arquivo.pdf> --map-id <MapID>")
        print(" python enhanced_pdf_extractor.py <arquivo.pdf> --map-id <MapID> <chunk_size>")
        print("\nEXEMPLOS:")
        print(" python enhanced_pdf_extractor.py documento.pdf")
        print(" python enhanced_pdf_extractor.py documento.pdf 'Pershing Square'")
        print(" python enhanced_pdf_extractor.py documento.pdf --map-id 123")
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
        elif sys.argv[i].isdigit():
            chunk_size = int(sys.argv[i])
            i += 1
        elif not fund_identifier and not map_id:
            fund_identifier = sys.argv[i]
            i += 1
        else:
            i += 1
    
    if not Path(pdf_file).exists():
        print(f"Arquivo não encontrado: {pdf_file}")
        return
    
    # Criar extrator e executar
    extractor = EnhancedPDFExtractor(chunk_size=chunk_size, overlap=overlap)
    result = extractor.extract_to_enhanced_chunks(
        pdf_file, 
        fund_identifier=fund_identifier, 
        map_id=map_id
    )
    
    if result:
        print(f"\n✅ EXTRAÇÃO MELHORADA CONCLUÍDA!")
        print(f"📈 Estrutura hierárquica detectada: {result['statistics']['structural_sections']} seções")
        print(f"🎯 Chunks alta qualidade: {result['statistics']['high_confidence_chunks']}")


if __name__ == "__main__":
    main()