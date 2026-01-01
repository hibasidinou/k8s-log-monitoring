#!/usr/bin/env python3
"""
Generate architecture diagram for the multi-server streaming pipeline.
"""

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
import matplotlib.patches as patches

def create_architecture_diagram():
    """Create the architecture diagram for the streaming pipeline."""
    
    fig, ax = plt.subplots(1, 1, figsize=(14, 10))
    ax.set_xlim(0, 10)
    ax.set_ylim(0, 12)
    ax.axis('off')
    
    # Colors
    color_input = '#4A90E2'      # Blue
    color_streaming = '#50C878'  # Green
    color_output = '#FF6B6B'     # Red
    color_arrow = '#2C3E50'      # Dark gray
    
    # Title
    title = ax.text(5, 11.5, 'Pipeline de Streaming Multi-Serveurs', 
                    ha='center', va='center', fontsize=20, fontweight='bold',
                    bbox=dict(boxstyle='round,pad=0.5', facecolor='#ECF0F1', edgecolor='#34495E', linewidth=2))
    
    # ==================== INPUT LAYER ====================
    input_box = FancyBboxPatch((2.5, 8.5), 5, 1.5,
                               boxstyle="round,pad=0.1",
                               facecolor=color_input, edgecolor='white', linewidth=2)
    ax.add_patch(input_box)
    
    input_title = ax.text(5, 10, 'INPUT LAYER', 
                          ha='center', va='center', fontsize=14, fontweight='bold', color='white')
    
    # CSV Reader
    csv_box = FancyBboxPatch((3, 8.7), 1.8, 0.8,
                             boxstyle="round,pad=0.05",
                             facecolor='white', edgecolor=color_input, linewidth=1.5)
    ax.add_patch(csv_box)
    ax.text(3.9, 9.1, 'CSV Reader', ha='center', va='center', fontsize=11, fontweight='bold')
    
    # Data Loader
    loader_box = FancyBboxPatch((5.2, 8.7), 1.8, 0.8,
                                boxstyle="round,pad=0.05",
                                facecolor='white', edgecolor=color_input, linewidth=1.5)
    ax.add_patch(loader_box)
    ax.text(6.1, 9.1, 'Data Loader', ha='center', va='center', fontsize=11, fontweight='bold')
    
    # Arrow from Input to Streaming
    arrow1 = FancyArrowPatch((5, 8.5), (5, 7),
                            arrowstyle='->', mutation_scale=25,
                            linewidth=3, color=color_arrow, zorder=1)
    ax.add_patch(arrow1)
    
    # ==================== STREAMING LAYER ====================
    streaming_box = FancyBboxPatch((1.5, 5), 7, 2,
                                   boxstyle="round,pad=0.1",
                                   facecolor=color_streaming, edgecolor='white', linewidth=2)
    ax.add_patch(streaming_box)
    
    streaming_title = ax.text(5, 6.7, 'STREAMING LAYER', 
                              ha='center', va='center', fontsize=14, fontweight='bold', color='white')
    
    # 10 Threads
    threads_box = FancyBboxPatch((2, 5.3), 1.8, 1.2,
                                boxstyle="round,pad=0.05",
                                facecolor='white', edgecolor=color_streaming, linewidth=1.5)
    ax.add_patch(threads_box)
    ax.text(2.9, 5.95, '10 Threads', ha='center', va='center', fontsize=11, fontweight='bold')
    ax.text(2.9, 5.6, '(Multi-Server)', ha='center', va='center', fontsize=9, style='italic')
    
    # Formatter
    formatter_box = FancyBboxPatch((4.1, 5.3), 1.8, 1.2,
                                   boxstyle="round,pad=0.05",
                                   facecolor='white', edgecolor=color_streaming, linewidth=1.5)
    ax.add_patch(formatter_box)
    ax.text(5, 5.95, 'Formatter', ha='center', va='center', fontsize=11, fontweight='bold')
    ax.text(5, 5.6, '(Log Parser)', ha='center', va='center', fontsize=9, style='italic')
    
    # Kafka Producer
    kafka_prod_box = FancyBboxPatch((6.2, 5.3), 1.8, 1.2,
                                    boxstyle="round,pad=0.05",
                                    facecolor='white', edgecolor=color_streaming, linewidth=1.5)
    ax.add_patch(kafka_prod_box)
    ax.text(7.1, 5.95, 'Kafka Prod', ha='center', va='center', fontsize=11, fontweight='bold')
    ax.text(7.1, 5.6, '(Publisher)', ha='center', va='center', fontsize=9, style='italic')
    
    # Arrow from Streaming to Output
    arrow2 = FancyArrowPatch((5, 5), (5, 3.5),
                            arrowstyle='->', mutation_scale=25,
                            linewidth=3, color=color_arrow, zorder=1)
    ax.add_patch(arrow2)
    
    # ==================== OUTPUT LAYER ====================
    output_box = FancyBboxPatch((2, 1), 6, 2,
                               boxstyle="round,pad=0.1",
                               facecolor=color_output, edgecolor='white', linewidth=2)
    ax.add_patch(output_box)
    
    output_title = ax.text(5, 2.7, 'OUTPUT LAYER', 
                          ha='center', va='center', fontsize=14, fontweight='bold', color='white')
    
    # Kafka Topic
    kafka_topic_box = FancyBboxPatch((2.5, 1.3), 2.5, 1.2,
                                     boxstyle="round,pad=0.05",
                                     facecolor='white', edgecolor=color_output, linewidth=1.5)
    ax.add_patch(kafka_topic_box)
    ax.text(3.75, 1.95, 'Kafka Topic', ha='center', va='center', fontsize=11, fontweight='bold')
    ax.text(3.75, 1.6, 'kubernetes-logs', ha='center', va='center', fontsize=9, style='italic', color='#555')
    
    # Log File
    logfile_box = FancyBboxPatch((5.5, 1.3), 2.5, 1.2,
                                 boxstyle="round,pad=0.05",
                                 facecolor='white', edgecolor=color_output, linewidth=1.5)
    ax.add_patch(logfile_box)
    ax.text(6.75, 1.95, 'Log File', ha='center', va='center', fontsize=11, fontweight='bold')
    ax.text(6.75, 1.6, 'streamed_logs.log', ha='center', va='center', fontsize=9, style='italic', color='#555')
    
    # ==================== Additional Details ====================
    # Data flow indicators
    ax.text(5, 7.75, '↓', ha='center', va='center', fontsize=20, color=color_arrow, fontweight='bold')
    ax.text(5, 4.25, '↓', ha='center', va='center', fontsize=20, color=color_arrow, fontweight='bold')
    
    # Footer with additional info
    footer_text = (
        "Architecture: 10 serveurs parallèles | "
        "Format: Logs structurés JSON + Texte | "
        "Temps réel simulé"
    )
    ax.text(5, 0.3, footer_text, ha='center', va='center', 
            fontsize=9, style='italic', color='#7F8C8D',
            bbox=dict(boxstyle='round,pad=0.3', facecolor='#F8F9FA', edgecolor='#BDC3C7', alpha=0.7))
    
    plt.tight_layout()
    
    # Save the diagram
    output_path = 'docs/presentation/architecture_diagram.png'
    import os
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
    print(f"✅ Diagram saved to: {output_path}")
    
    # Also save as PDF for better quality
    pdf_path = 'docs/presentation/architecture_diagram.pdf'
    plt.savefig(pdf_path, bbox_inches='tight', facecolor='white')
    print(f"✅ Diagram saved to: {pdf_path}")
    
    plt.show()

if __name__ == "__main__":
    create_architecture_diagram()

