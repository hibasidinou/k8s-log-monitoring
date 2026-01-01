#!/usr/bin/env python3
"""
Generate UML Class Diagram for stream_logs.py
"""

import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle, FancyArrowPatch
from matplotlib.lines import Line2D

def create_class_diagram():
    """Create UML Class Diagram for MultiServerLogStreamer."""
    
    fig, ax = plt.subplots(figsize=(14, 10))
    ax.set_xlim(0, 14)
    ax.set_ylim(0, 10)
    ax.axis('off')
    
    # Colors
    WHITE = '#FFFFFF'
    BLACK = '#000000'
    BLUE = '#2563EB'
    RED = '#DC2626'
    GREEN = '#059669'
    PURPLE = '#7C3AED'
    GRAY = '#6B7280'
    
    # Title
    title_rect = Rectangle((0.5, 9.2), 13, 0.6, facecolor=BLUE, edgecolor=BLACK, linewidth=2)
    ax.add_patch(title_rect)
    ax.text(7, 9.5, 'UML Class Diagram - stream_logs.py', 
            ha='center', va='center', fontsize=16, fontweight='bold', color='white')
    
    # ==================== MultiServerLogStreamer CLASS ====================
    cx, cy = 1, 4.5
    cw, ch = 6, 4
    
    # Main class box
    class_box = Rectangle((cx, cy), cw, ch, facecolor=WHITE, edgecolor=BLACK, linewidth=2)
    ax.add_patch(class_box)
    
    # Class name section
    name_box = Rectangle((cx, cy + ch - 0.5), cw, 0.5, facecolor=BLUE, edgecolor=BLACK, linewidth=2)
    ax.add_patch(name_box)
    sep1 = Line2D([cx, cx + cw], [cy + ch - 0.5, cy + ch - 0.5], color=BLACK, linewidth=2)
    ax.add_line(sep1)
    ax.text(cx + cw/2, cy + ch - 0.25, 'MultiServerLogStreamer', 
            ha='center', va='center', fontsize=13, fontweight='bold', color='white')
    
    # Attributes section
    attr_y = cy + ch - 0.5 - 0.35
    sep2 = Line2D([cx, cx + cw], [attr_y, attr_y], color=BLACK, linewidth=2)
    ax.add_line(sep2)
    
    attrs = [
        '- df : pandas.DataFrame',
        '- num_servers : int',
        '- log_file : file',
        '- log_lock : threading.Lock',
        '- producer : KafkaProducer'
    ]
    
    for i, attr in enumerate(attrs):
        y_pos = attr_y - 0.35 - i * 0.32
        ax.text(cx + 0.2, y_pos, attr, ha='left', va='center', 
               fontsize=9.5, family='monospace')
    
    # Methods section
    method_start = attr_y - len(attrs) * 0.32 - 0.25
    sep3 = Line2D([cx, cx + cw], [method_start, method_start], color=BLACK, linewidth=2)
    ax.add_line(sep3)
    
    methods = [
        '+ __init__(csv_path, num_servers=10)',
        '+ format_log(row, server_id) : tuple',
        '+ stream_server_logs(server_id)',
        '+ start_streaming()'
    ]
    
    for i, method in enumerate(methods):
        y_pos = method_start - 0.35 - i * 0.32
        ax.text(cx + 0.2, y_pos, method, ha='left', va='center', 
               fontsize=9.5, family='monospace')
    
    # ==================== External Classes ====================
    ex_w, ex_h = 4, 1.5
    
    # KafkaProducer
    kx, ky = 9, 7.2
    k_box = Rectangle((kx, ky), ex_w, ex_h, facecolor=WHITE, edgecolor=RED, linewidth=2)
    ax.add_patch(k_box)
    k_name = Rectangle((kx, ky + ex_h - 0.35), ex_w, 0.35, facecolor=RED, edgecolor=RED)
    ax.add_patch(k_name)
    Line2D([kx, kx + ex_w], [ky + ex_h - 0.35, ky + ex_h - 0.35], color=RED, linewidth=2).axes = ax
    ax.text(kx + ex_w/2, ky + ex_h - 0.175, 'KafkaProducer', 
            ha='center', va='center', fontsize=11, fontweight='bold', color='white')
    
    # Thread
    tx, ty = 9, 5.2
    t_box = Rectangle((tx, ty), ex_w, ex_h, facecolor=WHITE, edgecolor=GREEN, linewidth=2)
    ax.add_patch(t_box)
    t_name = Rectangle((tx, ty + ex_h - 0.35), ex_w, 0.35, facecolor=GREEN, edgecolor=GREEN)
    ax.add_patch(t_name)
    Line2D([tx, tx + ex_w], [ty + ex_h - 0.35, ty + ex_h - 0.35], color=GREEN, linewidth=2).axes = ax
    ax.text(tx + ex_w/2, ty + ex_h - 0.175, 'Thread', 
            ha='center', va='center', fontsize=11, fontweight='bold', color='white')
    
    # DataFrame
    dx, dy = 9, 3.2
    d_box = Rectangle((dx, dy), ex_w, ex_h, facecolor=WHITE, edgecolor=PURPLE, linewidth=2)
    ax.add_patch(d_box)
    d_name = Rectangle((dx, dy + ex_h - 0.35), ex_w, 0.35, facecolor=PURPLE, edgecolor=PURPLE)
    ax.add_patch(d_name)
    Line2D([dx, dx + ex_w], [dy + ex_h - 0.35, dy + ex_h - 0.35], color=PURPLE, linewidth=2).axes = ax
    ax.text(dx + ex_w/2, dy + ex_h - 0.175, 'DataFrame', 
            ha='center', va='center', fontsize=11, fontweight='bold', color='white')
    
    # ==================== Relationships ====================
    # MultiServerLogStreamer -> KafkaProducer
    arrow1 = FancyArrowPatch((cx + cw, cy + ch - 0.7), (kx, ky + ex_h/2),
                            arrowstyle='->', mutation_scale=20, linewidth=2, color=GRAY)
    ax.add_patch(arrow1)
    ax.text((cx + cw + kx)/2 - 0.2, cy + ch - 0.4, 'uses', ha='center', va='center',
           fontsize=9, fontweight='bold', color=GRAY)
    
    # MultiServerLogStreamer -> Thread
    arrow2 = FancyArrowPatch((cx + cw, cy + ch - 1.8), (tx, ty + ex_h/2),
                            arrowstyle='->', mutation_scale=20, linewidth=2, color=GRAY)
    ax.add_patch(arrow2)
    ax.text((cx + cw + tx)/2 - 0.2, cy + ch - 1.5, 'creates', ha='center', va='center',
           fontsize=9, fontweight='bold', color=GRAY)
    
    # MultiServerLogStreamer -> DataFrame
    arrow3 = FancyArrowPatch((cx + cw, cy + ch - 2.9), (dx, dy + ex_h/2),
                            arrowstyle='->', mutation_scale=20, linewidth=2, color=GRAY)
    ax.add_patch(arrow3)
    ax.text((cx + cw + dx)/2 - 0.2, cy + ch - 2.6, 'uses', ha='center', va='center',
           fontsize=9, fontweight='bold', color=GRAY)
    
    # Legend
    legend_x, legend_y = 1, 0.5
    legend_box = Rectangle((legend_x, legend_y), 5, 1.5, 
                          facecolor='#F9FAFB', edgecolor=GRAY, linewidth=1)
    ax.add_patch(legend_box)
    ax.text(legend_x + 2.5, legend_y + 1.2, 'Légende', 
            ha='center', va='center', fontsize=11, fontweight='bold')
    ax.text(legend_x + 0.3, legend_y + 0.75, '-  Attribut privé', 
            ha='left', va='center', fontsize=9, family='monospace')
    ax.text(legend_x + 0.3, legend_y + 0.4, '+  Méthode publique', 
            ha='left', va='center', fontsize=9, family='monospace')
    
    plt.tight_layout()
    
    # Save
    import os
    output_dir = 'docs/presentation'
    os.makedirs(output_dir, exist_ok=True)
    
    png_path = os.path.join(output_dir, 'class_diagram.png')
    plt.savefig(png_path, dpi=300, bbox_inches='tight', facecolor='white', pad_inches=0.2)
    print(f"✅ Diagram saved to: {png_path}")
    
    pdf_path = os.path.join(output_dir, 'class_diagram.pdf')
    plt.savefig(pdf_path, bbox_inches='tight', facecolor='white', pad_inches=0.2)
    print(f"✅ Diagram saved to: {pdf_path}")
    
    plt.show()

if __name__ == "__main__":
    create_class_diagram()
