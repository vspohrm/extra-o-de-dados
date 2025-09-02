#!/usr/bin/env python3
"""
Detector de Gráficos em PDF - Versão Final
Foco na precisão e robustez
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


class PreciseChartDetector:
    """Detector preciso de gráficos com foco em padrões específicos."""

    @staticmethod
    def analyze_page_for_charts(doc, page_num: int) -> dict:
        """Análise completa de uma página para detectar gráficos."""
        page = doc[page_num]
        print(f"\n🔍 Analisando página {page_num + 1}")
        print(f"📐 Dimensões: {page.rect.width:.0f} x {page.rect.height:.0f}")

        analysis = {
            "page_number": page_num + 1,
            "chart_score": 0.0,
            "indicators": [],
            "elements": {
                "images": {"count": 0, "large_count": 0, "details": []},
                "vectors": {"count": 0, "lines_h": 0, "lines_v": 0, "rectangles": 0},
                "text": {"numbers": [], "keywords": [], "total_words": 0}
            },
            "conclusion": ""
        }

        # 1. ANÁLISE DE IMAGENS
        images = page.get_images(full=True)
        analysis["elements"]["images"]["count"] = len(images)
        print(f"📸 Imagens encontradas: {len(images)}")

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
                    "index": i,
                    "dimensions": f"{pix.width}x{pix.height}",
                    "pixels": pixels,
                    "aspect_ratio": aspect,
                    "size_mb": round(len(pix.tobytes()) / 1024 / 1024, 3)
                }
                print(f" Img {i+1}: {img_detail['dimensions']} ({pixels:,} px, ratio: {aspect})")

                # Classificar imagem
                if pixels > 100000:  # Grande
                    analysis["elements"]["images"]["large_count"] += 1
                    img_detail["category"] = "large"

                    # Características típicas de gráfico
                    chart_features = 0
                    if 50000 <= pixels <= 800000:  # Tamanho típico de gráfico
                        chart_features += 1
                    if 0.6 <= aspect <= 2.5:  # Proporção típica
                        chart_features += 1
                    if pix.width > 300 and pix.height > 200:  # Dimensões substanciais
                        chart_features += 1
                    img_detail["chart_features"] = chart_features

                    if chart_features >= 2:
                        print(f" 🎯 FORTE CANDIDATO A GRÁFICO! ({chart_features}/3 características)")
                        analysis["chart_score"] += 0.4
                        analysis["indicators"].append(f"large_chart_image_{i+1}")
                elif pixels > 20000:
                    img_detail["category"] = "medium"
                    print(f" 📊 Possível gráfico médio")
                    analysis["chart_score"] += 0.2
                else:
                    img_detail["category"] = "small"
                    print(f" 🔸 Imagem pequena")

                analysis["elements"]["images"]["details"].append(img_detail)
                pix = None
            except Exception as e:
                print(f" ❌ Erro na imagem {i}: {e}")
                analysis["elements"]["images"]["details"].append({
                    "index": i, "error": str(e)
                })

        # 2. ANÁLISE DE ELEMENTOS VETORIAIS
        drawings = page.get_drawings()
        analysis["elements"]["vectors"]["count"] = len(drawings)
        print(f"🎨 Elementos vetoriais: {len(drawings)}")

        all_lines_h = []
        all_lines_v = []
        rectangles = []

        for drawing in drawings:
            items = drawing.get("items", [])
            for item in items:
                if not item or len(item) < 2:
                    continue
                item_type = item[0]
                coords = item[1]
                if item_type == "l" and len(coords) >= 4:  # Linha
                    x1, y1, x2, y2 = coords[0], coords[1], coords[2], coords[3]
                    dx, dy = abs(x2 - x1), abs(y2 - y1)
                    length = (dx**2 + dy**2)**0.5
                    if dy < 5 and dx > 30:  # Linha horizontal
                        all_lines_h.append({
                            "y": round((y1 + y2) / 2, 1),
                            "x_start": round(min(x1, x2), 1),
                            "x_end": round(max(x1, x2), 1),
                            "length": round(length, 1)
                        })
                        analysis["elements"]["vectors"]["lines_h"] += 1
                    elif dx < 5 and dy > 30:  # Linha vertical
                        all_lines_v.append({
                            "x": round((x1 + x2) / 2, 1),
                            "y_start": round(min(y1, y2), 1),
                            "y_end": round(max(y1, y2), 1),
                            "length": round(length, 1)
                        })
                        analysis["elements"]["vectors"]["lines_v"] += 1
                elif item_type == "re":  # Retângulo
                    rectangles.append(coords)
                    analysis["elements"]["vectors"]["rectangles"] += 1

        print(f" Linhas H: {len(all_lines_h)}, V: {len(all_lines_v)}, Retângulos: {len(rectangles)}")

        # 3. DETECTAR PADRÕES DE EIXOS
        axes_detected = False
        grid_detected = False

        # Eixos perpendiculares
        intersections = 0
        for h_line in all_lines_h:
            for v_line in all_lines_v:
                # Verificar interseção
                if (h_line["x_start"] <= v_line["x"] <= h_line["x_end"] and
                        v_line["y_start"] <= h_line["y"] <= v_line["y_end"]):
                    intersections += 1

        if intersections > 0:
            axes_detected = True
            analysis["chart_score"] += 0.3
            analysis["indicators"].append("perpendicular_axes")
            print(f" 🎯 Eixos perpendiculares detectados! ({intersections} interseções)")

        # Grade (múltiplas linhas paralelas)
        if len(all_lines_h) >= 3 and len(all_lines_v) >= 2:
            grid_detected = True
            analysis["chart_score"] += 0.25
            analysis["indicators"].append("grid_pattern")
            print(f" 📊 Grade detectada!")

        # Padrão de barras
        if len(rectangles) >= 3:
            analysis["chart_score"] += 0.2
            analysis["indicators"].append("bar_pattern")
            print(f" 📊 Padrão de barras detectado!")

        # 4. ANÁLISE DE TEXTO
        text = page.get_text()
        words = text.split() if text else []
        analysis["elements"]["text"]["total_words"] = len(words)

        # Detectar números isolados (labels de eixo)
        numbers = []
        for word in words:
            if re.match(r'^\d+(\.\d+)?$', word):
                numbers.append(word)
        analysis["elements"]["text"]["numbers"] = numbers

        # Palavras-chave de gráfico
        chart_keywords = [
            'gráfico', 'chart', 'figura', 'dados', 'média', 'total',
            'evolução', 'tendência', 'comparação', '%', 'percentual'
        ]
        keywords_found = []
        for word in words:
            for keyword in chart_keywords:
                if keyword.lower() in word.lower():
                    keywords_found.append(word)
        analysis["elements"]["text"]["keywords"] = list(set(keywords_found))

        print(f"📝 Texto: {len(words)} palavras, {len(numbers)} números")
        if len(numbers) >= 4:
            analysis["chart_score"] += 0.15
            analysis["indicators"].append("numeric_labels")
            print(f" 🔢 Labels numéricos: {', '.join(numbers[:8])}")
        if keywords_found:
            analysis["chart_score"] += 0.1
            analysis["indicators"].append("chart_keywords")
            print(f" 🏷️ Palavras-chave: {', '.join(keywords_found[:5])}")
        # Percentuais
        if '%' in text or 'percent' in text.lower():
            analysis["chart_score"] += 0.1
            analysis["indicators"].append("percentages")
            print(f" 📊 Percentuais detectados")

        # 5. ANÁLISE DE LAYOUT
        # Páginas de gráfico tendem a ter pouco texto
        if len(words) < 100 and (axes_detected or analysis["elements"]["images"]["large_count"] > 0):
            analysis["chart_score"] += 0.15
            analysis["indicators"].append("minimal_text_page")
            print(f" 📄 Layout típico de página de gráfico (pouco texto)")

        # 6. CONCLUSÃO
        final_score = round(min(analysis["chart_score"], 1.0), 3)
        analysis["chart_score"] = final_score
        if final_score >= 0.7:
            analysis["conclusion"] = "ALTA probabilidade de gráfico"
            confidence = "high"
        elif final_score >= 0.4:
            analysis["conclusion"] = "MÉDIA probabilidade de gráfico"
            confidence = "medium"
        else:
            analysis["conclusion"] = "BAIXA probabilidade de gráfico"
            confidence = "low"
        analysis["confidence"] = confidence
        print(f"📊 SCORE FINAL: {final_score}")
        print(f"🎯 {analysis['conclusion']}")
        return analysis


def analyze_specific_page(file_path: str, page_number: int, save_images: bool = True):
    """Análise focada em uma página específica."""
    print(f"🎯 ANÁLISE ESPECÍFICA - PÁGINA {page_number}")
    print(f"📄 Arquivo: {Path(file_path).name}")
    try:
        doc = fitz.open(file_path)
        if page_number > len(doc) or page_number < 1:
            print(f"❌ Página {page_number} não existe (total: {len(doc)})")
            return

        # Análise da página
        result = PreciseChartDetector.analyze_page_for_charts(doc, page_number - 1)

        # Salvar imagens se solicitado
        if save_images and result["elements"]["images"]["count"] > 0:
            page = doc[page_number - 1]
            output_dir = Path(file_path).parent / f"page_{page_number}_elements"
            output_dir.mkdir(exist_ok=True)
            images = page.get_images(full=True)
            for i, img in enumerate(images):
                try:
                    pix = fitz.Pixmap(doc, img[0])
                    if pix.colorspace and pix.colorspace.name in ["DeviceN", "Separation", "Lab", "ICCBased"]:
                        pix_rgb = fitz.Pixmap(fitz.csRGB, pix)
                        pix = pix_rgb
                    img_file = output_dir / f"element_{i+1}.png"
                    pix.save(str(img_file))
                    print(f"💾 Elemento salvo: {img_file}")
                    pix = None
                except Exception as e:
                    print(f"❌ Erro ao salvar elemento {i+1}: {e}")

        # Salvar análise
        analysis_file = Path(file_path).parent / f"chart_analysis_page_{page_number}.json"
        # Garantir que tudo é serializável
        clean_result = json.loads(json.dumps(result, default=str))
        with open(analysis_file, 'w', encoding='utf-8') as f:
            json.dump(clean_result, f, indent=2, ensure_ascii=False)
        print(f"\n💾 Análise salva em: {analysis_file}")
        doc.close()
        return result
    except Exception as e:
        print(f"❌ Erro: {e}")
        return None


def scan_all_pages(file_path: str):
    """Escaneia todas as páginas procurando gráficos."""
    print(f"🔍 ESCANEANDO TODO O DOCUMENTO")
    print(f"📄 Arquivo: {Path(file_path).name}")
    try:
        doc = fitz.open(file_path)
        print(f"📊 Total de páginas: {len(doc)}")
        results = {
            "document": Path(file_path).name,
            "total_pages": len(doc),
            "scan_timestamp": str(datetime.datetime.now()),
            "pages": {},
            "summary": {
                "chart_pages": [],
                "best_candidates": []
            }
        }

        # Analisar cada página
        for page_num in range(len(doc)):
            print(f"\n📄 Página {page_num + 1}:")
            analysis = PreciseChartDetector.analyze_page_for_charts(doc, page_num)
            page_key = f"page_{page_num + 1}"
            results["pages"][page_key] = analysis
            # Se tem alta probabilidade, adicionar aos candidatos
            if analysis["chart_score"] >= 0.4:
                results["summary"]["chart_pages"].append(page_num + 1)
                results["summary"]["best_candidates"].append({
                    "page": page_num + 1,
                    "score": analysis["chart_score"],
                    "confidence": analysis["confidence"],
                    "main_indicators": analysis["indicators"][:3]  # Top 3
                })

        # Ordenar candidatos por score
        results["summary"]["best_candidates"].sort(key=lambda x: x["score"], reverse=True)

        # Resumo final
        print(f"\n📋 RESUMO FINAL:")
        print(f" 📊 Páginas com gráficos prováveis: {len(results['summary']['chart_pages'])}")
        if results["summary"]["best_candidates"]:
            print(f" 🏆 Melhores candidatos:")
            for candidate in results["summary"]["best_candidates"][:3]:
                print(f" 📄 Página {candidate['page']}: score {candidate['score']:.3f} ({candidate['confidence']})")
                print(f" Indicadores: {', '.join(candidate['main_indicators'])}")

        # Salvar resultado
        output_file = Path(file_path).parent / f"scan_completo_{Path(file_path).stem}.json"
        # Garantir serialização
        clean_results = json.loads(json.dumps(results, default=str))
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(clean_results, f, indent=2, ensure_ascii=False)
        print(f"\n💾 Scan completo salvo em: {output_file}")
        doc.close()
        return results
    except Exception as e:
        print(f"❌ Erro no scan: {e}")
        return None


def extract_page_4_optimized(file_path: str):
    """Extração otimizada especificamente para página 4."""
    print("🎯 EXTRAÇÃO OTIMIZADA - PÁGINA 4")
    print("Focando em detectar o gráfico que você mencionou...")
    # Primeiro, análise rápida
    result = analyze_specific_page(file_path, 4, save_images=True)
    if result and result.get("chart_score", 0) >= 0.3:
        print(f"\n✅ GRÁFICO DETECTADO NA PÁGINA 4!")
        print(f" Score: {result['chart_score']}")
        print(f" Confiança: {result['confidence']}")
        print(f" Indicadores: {', '.join(result['indicators'])}")
        # Detalhes específicos
        images = result["elements"]["images"]
        if images["large_count"] > 0:
            print(f"\n📊 ELEMENTOS DO GRÁFICO:")
            for img in images["details"]:
                if img.get("category") == "large":
                    print(f" 🎯 Imagem principal: {img['dimensions']} ({img['pixels']:,} pixels)")
                    print(f" Proporção: {img['aspect_ratio']} (ideal para gráfico: 0.6-2.5)")
                    print(f" Tamanho: {img['size_mb']} MB")
        vectors = result["elements"]["vectors"]
        if vectors["lines_h"] > 0 or vectors["lines_v"] > 0:
            print(f" 📐 Estrutura vetorial:")
            print(f" Linhas horizontais: {vectors['lines_h']}")
            print(f" Linhas verticais: {vectors['lines_v']}")
            print(f" Retângulos: {vectors['rectangles']}")
    else:
        print(f"\n🤔 Detecção inconclusiva na página 4")
        print(f" Score: {result.get('chart_score', 0) if result else 'erro'}")
        print(" Pode ser um gráfico mais simples ou com padrão não convencional")


def main():
    """Função principal com comandos específicos."""
    print("🚀 Detector Preciso de Gráficos")
    if len(sys.argv) < 2:
        print("\n💡 COMANDOS DISPONÍVEIS:")
        print(" python detector.py <arquivo.pdf> # Scan completo")
        print(" python detector.py page4 <arquivo.pdf> # Foco na página 4")
        print(" python detector.py page <arquivo.pdf> <N> # Página específica")
        print(" python detector.py scan <arquivo.pdf> # Scan detalhado")
        return

    command = sys.argv[1]
    if command == "page4" and len(sys.argv) > 2:
        pdf_file = sys.argv[2]
        extract_page_4_optimized(pdf_file)
    elif command == "page" and len(sys.argv) > 3:
        pdf_file = sys.argv[2]
        page_num = int(sys.argv[3])
        analyze_specific_page(pdf_file, page_num)
    elif command == "scan" and len(sys.argv) > 2:
        pdf_file = sys.argv[2]
        scan_all_pages(pdf_file)
    else:
        # Comando padrão - scan rápido
        pdf_file = command
        if not Path(pdf_file).exists():
            print(f"❌ Arquivo não encontrado: {pdf_file}")
            return
        print(f"\n📄 Scan rápido: {Path(pdf_file).name}")
        try:
            doc = fitz.open(pdf_file)
            # Verificar cada página rapidamente
            chart_candidates = []
            for page_num in range(len(doc)):
                page = doc[page_num]
                # Análise rápida
                images = len(page.get_images())
                drawings = len(page.get_drawings())
                text_words = len(page.get_text().split())
                score = 0
                if images > 0 and images <= 3:  # Poucas imagens grandes
                    score += 0.3
                if drawings > 5:  # Muitos elementos vetoriais
                    score += 0.3
                if text_words < 150:  # Pouco texto
                    score += 0.2
                if score >= 0.4:
                    chart_candidates.append({
                        "page": page_num + 1,
                        "score": round(score, 2),
                        "images": images,
                        "vectors": drawings,
                        "words": text_words
                    })

            print(f"\n📊 CANDIDATOS A GRÁFICO:")
            if chart_candidates:
                chart_candidates.sort(key=lambda x: x["score"], reverse=True)
                for candidate in chart_candidates:
                    print(f" 📄 Página {candidate['page']}: score {candidate['score']}")
                    print(f" 📸 {candidate['images']} img, 🎨 {candidate['vectors']} vetores, 📝 {candidate['words']} palavras")
                # Análise detalhada da melhor
                best = chart_candidates[0]
                print(f"\n🔍 Analisando melhor candidato (página {best['page']})...")
                analyze_specific_page(pdf_file, best["page"], save_images=True)
            else:
                print(" ❓ Nenhum candidato óbvio encontrado")
                print(" Tente: python detector.py scan arquivo.pdf")
            doc.close()
        except Exception as e:
            print(f"❌ Erro: {e}")


if __name__ == "__main__":
    print("\n💡 EXEMPLO PARA SEU CASO:")
    print(" python detector.py page4 paginas.pdf")
    print()
    main()
