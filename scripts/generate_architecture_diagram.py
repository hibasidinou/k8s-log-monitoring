#!/usr/bin/env python3
"""
Generate Architecture Diagram showing the complete data flow pipeline.
"""

import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle, FancyBboxPatch, FancyArrowPatch, Circle
from matplotlib.lines import Line2D

def create_architecture_diagram():
    """Create architecture diagram showing data flow through layers."""
    
    fig, ax = plt.subplots(figsize=(18, 14))
    ax.set_xlim(0, 18)
    ax.set_ylim(0, 14)
    ax.axis('off')
    
    # Colors
    SIMULATION_COLOR = '#3B82F6'  # Blue
    KAFKA_COLOR = '#10B981'       # Green
    SPARK_COLOR = '#F59E0B'       # Orange
    STORAGE_COLOR = '#8B5CF6'     # Purple
    BG_COLOR = '#F9FAFB'
    TEXT_COLOR = '#111827'
    
    # ==================== LAYER 1: SIMULATION ====================
    layer1_y = 11.5
    layer1_height = 1.8
    
    layer1_box = FancyBboxPatch((0.5, layer1_y), 17, layer1_height,
                               boxstyle="round,pad=0.1", linewidth=2.5,
                               facecolor=SIMULATION_COLOR, edgecolor='white')
    ax.add_patch(layer1_box)
    
    # Layer title
    title_bg = Rectangle((0.5, layer1_y + layer1_height - 0.4), 17, 0.4,
                        facecolor='#1E40AF', edgecolor='white', linewidth=1)
    ax.add_patch(title_bg)
    ax.text(9, layer1_y + layer1_height - 0.2, 
            'COUCHE SIMULATION', ha='center', va='center',
            fontsize=14, fontweight='bold', color='white')
    
    # Servers (10 threads)
    server_width = 1.3
    server_height = 1
    server_spacing = 0.2
    total_servers_width = 10 * server_width + 9 * server_spacing
    server_start_x = (18 - total_servers_width) / 2
    
    for i in range(10):
        server_x = server_start_x + i * (server_width + server_spacing)
        server_box = Rectangle((server_x, layer1_y + 0.15), server_width, server_height,
                              facecolor='white', edgecolor=SIMULATION_COLOR, linewidth=1.5)
        ax.add_patch(server_box)
        
        server_id = f'server-{i+1:02d}'
        ax.text(server_x + server_width/2, layer1_y + 0.75, server_id,
               ha='center', va='center', fontsize=9, fontweight='bold')
        ax.text(server_x + server_width/2, layer1_y + 0.45, f'Thread {i+1}',
               ha='center', va='center', fontsize=8)
        ax.text(server_x + server_width/2, layer1_y + 0.25, '88k logs',
               ha='center', va='center', fontsize=7, style='italic', color='#666')
        
        # Arrow down from server
        if i == 0 or i == 9:
            arrow = FancyArrowPatch((server_x + server_width/2, layer1_y + 0.15),
                                   (server_x + server_width/2, layer1_y - 0.2),
                                   arrowstyle='->', mutation_scale=15, linewidth=1.5,
                                   color='#666', zorder=10)
            ax.add_patch(arrow)
    
    # Merge arrows
    merge_line = Line2D([server_start_x + server_width/2, 
                        server_start_x + 10*server_width + 9*server_spacing - server_width/2],
                       [layer1_y - 0.2, layer1_y - 0.2],
                       color='#666', linewidth=2, zorder=5)
    ax.add_line(merge_line)
    
    # Single arrow down
    main_arrow1 = FancyArrowPatch((9, layer1_y - 0.2), (9, 10.2),
                                 arrowstyle='->', mutation_scale=25, linewidth=3,
                                 color='#666', zorder=10)
    ax.add_patch(main_arrow1)
    
    # ==================== LAYER 2: KAFKA ====================
    layer2_y = 8.5
    layer2_height = 1.5
    
    layer2_box = FancyBboxPatch((0.5, layer2_y), 17, layer2_height,
                               boxstyle="round,pad=0.1", linewidth=2.5,
                               facecolor=KAFKA_COLOR, edgecolor='white')
    ax.add_patch(layer2_box)
    
    # Layer title
    title_bg2 = Rectangle((0.5, layer2_y + layer2_height - 0.4), 17, 0.4,
                         facecolor='#059669', edgecolor='white', linewidth=1)
    ax.add_patch(title_bg2)
    ax.text(9, layer2_y + layer2_height - 0.2,
           'COUCHE MESSAGING (Kafka)', ha='center', va='center',
           fontsize=14, fontweight='bold', color='white')
    
    # Topic box
    topic_box = FancyBboxPatch((2, layer2_y + 0.2), 14, 0.9,
                              boxstyle="round,pad=0.05", linewidth=1.5,
                              facecolor='white', edgecolor=KAFKA_COLOR)
    ax.add_patch(topic_box)
    ax.text(9, layer2_y + 1.0, 'Topic: kubernetes-logs',
           ha='center', va='center', fontsize=11, fontweight='bold')
    
    # Partitions (3 partitions)
    part_width = 4
    part_height = 0.6
    part_spacing = 0.5
    total_part_width = 3 * part_width + 2 * part_spacing
    part_start_x = (18 - total_part_width) / 2
    
    servers_per_partition = [
        ['server-01', 'server-04', 'server-07', 'server-10'],
        ['server-02', 'server-05', 'server-08'],
        ['server-03', 'server-06', 'server-09']
    ]
    
    for i in range(3):
        part_x = part_start_x + i * (part_width + part_spacing)
        part_box = Rectangle((part_x, layer2_y + 0.25), part_width, part_height,
                            facecolor='#ECFDF5', edgecolor=KAFKA_COLOR, linewidth=1.5)
        ax.add_patch(part_box)
        ax.text(part_x + part_width/2, layer2_y + 0.7, f'Partition {i}',
               ha='center', va='center', fontsize=9, fontweight='bold')
        
        servers_text = '\n'.join(servers_per_partition[i])
        ax.text(part_x + part_width/2, layer2_y + 0.4, servers_text,
               ha='center', va='center', fontsize=7.5)
    
    # Arrow down
    main_arrow2 = FancyArrowPatch((9, layer2_y), (9, 7.5),
                                 arrowstyle='->', mutation_scale=25, linewidth=3,
                                 color='#666', zorder=10)
    ax.add_patch(main_arrow2)
    
    # ==================== LAYER 3: SPARK ====================
    layer3_y = 5
    layer3_height = 2
    
    layer3_box = FancyBboxPatch((0.5, layer3_y), 17, layer3_height,
                               boxstyle="round,pad=0.1", linewidth=2.5,
                               facecolor=SPARK_COLOR, edgecolor='white')
    ax.add_patch(layer3_box)
    
    # Layer title
    title_bg3 = Rectangle((0.5, layer3_y + layer3_height - 0.4), 17, 0.4,
                         facecolor='#D97706', edgecolor='white', linewidth=1)
    ax.add_patch(title_bg3)
    ax.text(9, layer3_y + layer3_height - 0.2,
           'COUCHE TRAITEMENT (Spark Streaming)', ha='center', va='center',
           fontsize=14, fontweight='bold', color='white')
    
    # Spark Consumer box
    consumer_box = FancyBboxPatch((2.5, layer3_y + 1.2), 13, 0.35,
                                 boxstyle="round,pad=0.05", linewidth=1.5,
                                 facecolor='white', edgecolor=SPARK_COLOR)
    ax.add_patch(consumer_box)
    ax.text(9, layer3_y + 1.38, 'Spark Consumer (micro-batch 10s)',
           ha='center', va='center', fontsize=10, fontweight='bold')
    
    # Executors (3 executors)
    exec_width = 3.5
    exec_height = 0.5
    exec_spacing = 0.8
    total_exec_width = 3 * exec_width + 2 * exec_spacing
    exec_start_x = (18 - total_exec_width) / 2
    
    for i in range(3):
        exec_x = exec_start_x + i * (exec_width + exec_spacing)
        exec_box = Rectangle((exec_x, layer3_y + 0.6), exec_width, exec_height,
                            facecolor='#FEF3C7', edgecolor=SPARK_COLOR, linewidth=1.5)
        ax.add_patch(exec_box)
        ax.text(exec_x + exec_width/2, layer3_y + 0.85, f'Executor {i+1}',
               ha='center', va='center', fontsize=9, fontweight='bold')
        ax.text(exec_x + exec_width/2, layer3_y + 0.7, f'Part. {i}',
               ha='center', va='center', fontsize=8)
    
    # Merge line
    merge_line2 = Line2D([exec_start_x + exec_width/2,
                         exec_start_x + 3*exec_width + 2*exec_spacing - exec_width/2],
                        [layer3_y + 0.6, layer3_y + 0.6],
                        color='#666', linewidth=2)
    ax.add_line(merge_line2)
    
    # DataFrame Operations box
    ops_box = FancyBboxPatch((5, layer3_y + 0.15), 8, 0.35,
                            boxstyle="round,pad=0.05", linewidth=1.5,
                            facecolor='white', edgecolor=SPARK_COLOR)
    ax.add_patch(ops_box)
    ax.text(9, layer3_y + 0.33, 'DataFrame Operations: Parse JSON • Validate • Aggregate • Detect Anomalies',
           ha='center', va='center', fontsize=9)
    
    # Arrow down
    main_arrow3 = FancyArrowPatch((9, layer3_y), (9, 4.2),
                                 arrowstyle='->', mutation_scale=25, linewidth=3,
                                 color='#666', zorder=10)
    ax.add_patch(main_arrow3)
    
    # ==================== LAYER 4: STORAGE ====================
    layer4_y = 1.5
    layer4_height = 2
    
    layer4_box = FancyBboxPatch((0.5, layer4_y), 17, layer4_height,
                               boxstyle="round,pad=0.1", linewidth=2.5,
                               facecolor=STORAGE_COLOR, edgecolor='white')
    ax.add_patch(layer4_box)
    
    # Layer title
    title_bg4 = Rectangle((0.5, layer4_y + layer4_height - 0.4), 17, 0.4,
                         facecolor='#7C3AED', edgecolor='white', linewidth=1)
    ax.add_patch(title_bg4)
    ax.text(9, layer4_y + layer4_height - 0.2,
           'COUCHE STOCKAGE', ha='center', va='center',
           fontsize=14, fontweight='bold', color='white')
    
    # Storage boxes (3 storage types)
    storage_width = 4.5
    storage_height = 1.2
    storage_spacing = 0.75
    total_storage_width = 3 * storage_width + 2 * storage_spacing
    storage_start_x = (18 - total_storage_width) / 2
    
    storage_types = [
        ('HDFS/S3', 'Raw logs'),
        ('Database', 'Aggregates'),
        ('Dashboard', 'Real-time')
    ]
    
    for i, (name, desc) in enumerate(storage_types):
        storage_x = storage_start_x + i * (storage_width + storage_spacing)
        storage_box = Rectangle((storage_x, layer4_y + 0.25), storage_width, storage_height,
                               facecolor='white', edgecolor=STORAGE_COLOR, linewidth=1.5)
        ax.add_patch(storage_box)
        ax.text(storage_x + storage_width/2, layer4_y + 1.1, name,
               ha='center', va='center', fontsize=11, fontweight='bold')
        ax.text(storage_x + storage_width/2, layer4_y + 0.6, desc,
               ha='center', va='center', fontsize=9, style='italic', color='#666')
    
    # Arrows to storage
    for i in range(3):
        storage_x = storage_start_x + i * (storage_width + storage_spacing) + storage_width/2
        arrow = FancyArrowPatch((9, 4.2), (storage_x, layer4_y + layer4_height),
                               arrowstyle='->', mutation_scale=20, linewidth=2,
                               color='#666', zorder=10, connectionstyle="arc3,rad=0.15")
        ax.add_patch(arrow)
    
    plt.tight_layout(pad=0.5)
    
    # Save
    import os
    output_dir = 'docs/presentation'
    os.makedirs(output_dir, exist_ok=True)
    
    png_path = os.path.join(output_dir, 'architecture_diagram.png')
    plt.savefig(png_path, dpi=300, bbox_inches='tight', facecolor='white', pad_inches=0.3)
    print(f"✅ Architecture diagram saved to: {png_path}")
    
    pdf_path = os.path.join(output_dir, 'architecture_diagram.pdf')
    plt.savefig(pdf_path, bbox_inches='tight', facecolor='white', pad_inches=0.3)
    print(f"✅ Architecture diagram saved to: {pdf_path}")
    
    plt.show()

if __name__ == "__main__":
    create_architecture_diagram()
