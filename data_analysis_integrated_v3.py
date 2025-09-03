import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from scipy import stats
import os
import sys
from datetime import datetime

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei']
plt.rcParams['axes.unicode_minus'] = False

# 定义需要排除的列
EXCLUDE_COLS = ["DUT_DID", "充电信息_充电座", "echo1_地毯", "LDS下降堵转次数检测"]

def is_valid_number(value):
    """检查值是否为有效数字"""
    if pd.isna(value):
        return False
    try:
        float(value)
        return True
    except (ValueError, TypeError):
        return False

def filter_valid_columns(df):
    """根据条件筛选有效列（集成data_filter_v2的筛选逻辑）"""
    valid_columns = []
    
    exclude_first_row_values = ["充电信息_充电座", "echo1_地毯", "DUT_DID", "LDS下降堵转次数检测"]
    for col in df.columns:
        # 排除指定列名
        if col in EXCLUDE_COLS:
            continue
        # 剔除第一行为指定内容的列
        if len(df) > 0 and str(df.iloc[0, df.columns.get_loc(col)]) in exclude_first_row_values:
            continue
        # 检查第二行（索引1）和第三行（索引2）是否为有效数字且不相等
        if len(df) >= 3:
            row1_val = df.iloc[1, df.columns.get_loc(col)]
            row2_val = df.iloc[2, df.columns.get_loc(col)]
            if not (is_valid_number(row1_val) and is_valid_number(row2_val)):
                continue
            if float(row1_val) == float(row2_val):
                continue
        else:
            continue
        valid_columns.append(col)
    
    return valid_columns

def read_and_filter_excel_data(file_path):
    """读取并筛选Excel文件数据（集成筛选功能）"""
    try:
        # 读取Excel文件，指定engine参数为'openpyxl'以支持xlsx格式
        df = pd.read_excel(file_path, engine='openpyxl')
        
        if len(df) < 4:
            raise ValueError("Excel文件行数不足，至少需要4行数据")
        
        # 筛选有效列
        valid_columns = filter_valid_columns(df)
        
        if not valid_columns:
            raise ValueError("没有找到符合条件的有效列")
        
        print(f"数据筛选: {len(df.columns)}列 → {len(valid_columns)}列")
        
        # 仅保留有效列的数据
        filtered_df = df[valid_columns]
        
        # 提取数据信息
        data_info = filtered_df.iloc[0]  # 第一行：数据信息
        lower_limits = filtered_df.iloc[1]  # 第二行：下限
        upper_limits = filtered_df.iloc[2]  # 第三行：上限
        raw_data = filtered_df.iloc[3:]  # 第四行及以后：原始数据
        
        return data_info, lower_limits, upper_limits, raw_data, valid_columns, df
        
    except Exception as e:
        print(f"读取Excel文件时发生错误: {str(e)}")
        return None, None, None, None, None, None

def process_data(raw_data):
    """处理数据：转换为数值类型并去除空值"""
    processed_data = {}
    
    for col in raw_data.columns:
        # 跳过排除的列
        if col in EXCLUDE_COLS:
            continue
            
        # 转换为数值类型
        numeric_data = pd.to_numeric(raw_data[col], errors='coerce')
        # 去除空值
        clean_data = numeric_data.dropna()
        
        if len(clean_data) > 0:
            processed_data[col] = clean_data
    
    return processed_data

def calculate_statistics(data):
    """计算数据的均值和标准差"""
    stats_dict = {}
    
    for col, values in data.items():
        # 跳过排除的列
        if col in EXCLUDE_COLS:
            continue
            
        try:
            mean_val = np.mean(values)
            std_val = np.std(values)
            
            # 检查计算结果是否有效
            if np.isnan(mean_val) or np.isnan(std_val) or np.isinf(mean_val) or np.isinf(std_val):
                print(f"警告: 列 {col} 的统计计算出现无效值，跳过此列")
                continue
                
            stats_dict[col] = {
                'mean': mean_val,
                'std': std_val,
                'data': values
            }
        except Exception as e:
            print(f"警告: 列 {col} 的统计计算失败: {e}，跳过此列")
            continue
    
    return stats_dict

def analyze_data_quality(stats_dict, data_info, lower_limits, upper_limits, raw_data, original_df):
    """分析数据质量，识别警告数据和超限数据"""
    warning_records = []
    out_of_limit_records = []
    
    for col, stats_data in stats_dict.items():
        # 跳过排除的列
        if col in EXCLUDE_COLS:
            continue
            
        data_name = data_info[col]
        lower_limit = lower_limits.get(col, None)
        upper_limit = upper_limits.get(col, None)
        data = stats_data['data']
        
        # 获取原始数据中该列的完整数据（包括索引信息）
        original_col_data = raw_data[col]
        
        # 确保上下限是数值类型
        try:
            if pd.notna(lower_limit):
                lower_limit = float(lower_limit)
            if pd.notna(upper_limit):
                upper_limit = float(upper_limit)
        except (ValueError, TypeError):
            print(f"警告: 列 {col} 的上下限不是有效数值，跳过此列")
            continue
        
        # 确保原始数据是数值类型
        try:
            numeric_col_data = pd.to_numeric(original_col_data, errors='coerce')
            # 只保留有效的数值数据
            valid_mask = pd.notna(numeric_col_data)
            numeric_col_data = numeric_col_data[valid_mask]
        except Exception as e:
            print(f"警告: 列 {col} 的数据转换失败: {e}，跳过此列")
            continue
        
        if pd.notna(lower_limit) and pd.notna(upper_limit) and upper_limit > lower_limit:
            range_total = upper_limit - lower_limit
            if range_total > 0:
                warning_range = 0.05 * range_total
                warning_low_end = lower_limit + warning_range
                warning_high_start = upper_limit - warning_range
                
                if warning_low_end < warning_high_start:
                    # 警告数据：距离上下限5%范围内，但不包括超过上下限的数据
                    warning_mask = ((numeric_col_data >= lower_limit) & (numeric_col_data <= warning_low_end)) | \
                                  ((numeric_col_data >= warning_high_start) & (numeric_col_data <= upper_limit))
                    
                    # 超限数据：超过上下限的数据
                    out_of_limit_mask = (numeric_col_data < lower_limit) | (numeric_col_data > upper_limit)
                    
                    # 获取警告数据的详细信息
                    warning_data = numeric_col_data[warning_mask]
                    for idx, value in warning_data.items():
                        # 获取SN条码（假设第一列是SN条码）
                        # 使用numeric_col_data的索引直接获取SN条码
                        try:
                            # 检查索引是否在有效范围内
                            if idx + 3 < len(original_df) and len(original_df.columns) > 0:
                                sn_code = original_df.iloc[idx + 3, 0]
                                row_number = idx + 4  # 原始Excel中的行号（从1开始）
                            else:
                                sn_code = "未知"
                                row_number = -1
                        except Exception as e:
                            print(f"警告: 获取SN条码失败: {e}")
                            sn_code = "未知"
                            row_number = -1
                        
                        # 判断警告原因
                        if value <= warning_low_end:
                            reason = f"接近下限 (距离下限: {value - lower_limit:.1f})"
                        else:
                            reason = f"接近上限 (距离上限: {upper_limit - value:.1f})"
                        
                        warning_records.append({
                            'SN条码': sn_code,
                            '数据名称': data_name,
                            '列名': col,
                            '数值': value,
                            '下限': lower_limit,
                            '上限': upper_limit,
                            '警告原因': reason,
                            '行号': row_number
                        })
                    
                    # 获取超限数据的详细信息
                    out_of_limit_data = numeric_col_data[out_of_limit_mask]
                    for idx, value in out_of_limit_data.items():
                        # 获取SN条码
                        # 使用numeric_col_data的索引直接获取SN条码
                        try:
                            # 检查索引是否在有效范围内
                            if idx + 3 < len(original_df) and len(original_df.columns) > 0:
                                sn_code = original_df.iloc[idx + 3, 0]
                                row_number = idx + 4  # 原始Excel中的行号（从1开始）
                            else:
                                sn_code = "未知"
                                row_number = -1
                        except Exception as e:
                            print(f"警告: 获取SN条码失败: {e}")
                            sn_code = "未知"
                            row_number = -1
                        
                        # 判断超限原因
                        if value < lower_limit:
                            reason = f"低于下限 (超出: {lower_limit - value:.1f})"
                        else:
                            reason = f"高于上限 (超出: {value - upper_limit:.1f})"
                        
                        out_of_limit_records.append({
                            'SN条码': sn_code,
                            '数据名称': data_name,
                            '列名': col,
                            '数值': value,
                            '下限': lower_limit,
                            '上限': upper_limit,
                            '超限原因': reason,
                            '行号': row_number
                        })
    
    return warning_records, out_of_limit_records

def create_excel_report(warning_records, out_of_limit_records, stats_dict, data_info, output_folder, raw_data):
    """创建Excel报告"""
    # 创建输出文件夹
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        print(f"已创建文件夹: {output_folder}")
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    excel_filename = f"Data_Analysis_Report_{timestamp}.xlsx"
    excel_path = os.path.join(output_folder, excel_filename)
    with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
        # 复测率统计（重写：第一列第5行及以下为SN信息，顺序重复即为复测）
        try:
            sn_list = raw_data.iloc[:, 0].dropna().astype(str).tolist()
            # 只统计第5行及以下（即raw_data的所有行）
            # 顺序遍历，统计复测SN及其复测次数
            sn_seen = {}
            sn_retest = {}
            for sn in sn_list:
                if sn in sn_seen:
                    sn_retest[sn] = sn_retest.get(sn, 1) + 1
                else:
                    sn_seen[sn] = True
            # 只保留复测（出现次数>1）的SN
            retest_sn_counts = {sn: count for sn, count in sn_retest.items() if count > 1}
            total_sn = len(sn_list)
            retest_sn_count = len(retest_sn_counts)
            retest_rate = retest_sn_count / total_sn if total_sn > 0 else 0
            retest_df = pd.DataFrame({
                '复测SN条码': [str(sn) for sn in retest_sn_counts.keys()],
                '复测次数': list(retest_sn_counts.values())
            })
        except Exception as e:
            print(f"复测率统计失败: {e}")
            total_sn = 0
            retest_sn_count = 0
            retest_rate = 0
            retest_df = pd.DataFrame(columns=['复测SN条码', '复测次数'])

        # 优化：警告数据sheet
        c_col_name = raw_data.columns[2] if raw_data.shape[1] >= 3 else ('C' if 'C' in raw_data.columns else None)
        def get_passfail(row_num):
            if c_col_name is not None and 0 <= row_num < len(raw_data):
                return raw_data.iat[row_num, raw_data.columns.get_loc(c_col_name)]
            return ''
        if warning_records:
            warning_df = pd.DataFrame(warning_records)
            warning_df = warning_df.drop(columns=[col for col in ['列名', '行号', 'PASS/FAIL'] if col in warning_df.columns])
            name_counts = warning_df['数据名称'].value_counts()
            warning_df['__count'] = warning_df['数据名称'].map(name_counts)
            warning_df = warning_df.sort_values(by='__count', ascending=False).drop(columns=['__count'])
            warning_df.to_excel(writer, sheet_name='警告数据', index=False)
        else:
            pd.DataFrame(columns=['SN条码', '数据名称', '数值', '下限', '上限', '警告原因']).to_excel(writer, sheet_name='警告数据', index=False)

        # 优化：超限数据sheet
        if out_of_limit_records:
            out_of_limit_df = pd.DataFrame(out_of_limit_records)
            out_of_limit_df = out_of_limit_df.drop(columns=[col for col in ['列名', '行号', 'PASS/FAIL'] if col in out_of_limit_df.columns])
            name_counts = out_of_limit_df['数据名称'].value_counts()
            out_of_limit_df['__count'] = out_of_limit_df['数据名称'].map(name_counts)
            out_of_limit_df = out_of_limit_df.sort_values(by='__count', ascending=False).drop(columns=['__count'])
            out_of_limit_df.to_excel(writer, sheet_name='超限数据', index=False)
        else:
            pd.DataFrame(columns=['SN条码', '数据名称', '数值', '下限', '上限', '超限原因']).to_excel(writer, sheet_name='超限数据', index=False)

        # 分组统计，分别输出三个sheet
        col_stats = {}
        for col, stats_data in stats_dict.items():
            # 跳过排除的列
            if col in EXCLUDE_COLS:
                continue
                
            data_name = data_info.get(col, col)
            # 强制转换上下限为 float，防止后续运算报错
            lower_limit = None
            upper_limit = None
            try:
                lower_limit = float(raw_data.iloc[1][col]) if pd.notna(raw_data.iloc[1][col]) else None
            except Exception:
                lower_limit = None
            try:
                upper_limit = float(raw_data.iloc[2][col]) if pd.notna(raw_data.iloc[2][col]) else None
            except Exception:
                upper_limit = None
            col_stats[col] = {
                '数据名称': data_name,
                '警告数据数': 0,
                '超限数据数': 0,
                '总数': 0,
                '下限': lower_limit,
                '上限': upper_limit
            }
        for rec in warning_records:
            col = rec.get('列名', None)
            if col in col_stats:
                col_stats[col]['警告数据数'] += 1
        for rec in out_of_limit_records:
            col = rec.get('列名', None)
            if col in col_stats:
                col_stats[col]['超限数据数'] += 1
        for col in col_stats:
            col_stats[col]['总数'] = col_stats[col]['警告数据数'] + col_stats[col]['超限数据数']

        col_stats_list = list(col_stats.values())
        # 警告数据排名
        warning_df = pd.DataFrame({
            '数据名称': [x['数据名称'] for x in col_stats_list],
            '警告数据数': [x['警告数据数'] for x in col_stats_list]
        })
        warning_df['警告数据排名'] = warning_df['警告数据数'].rank(method='min', ascending=False).astype(int)
        warning_df = warning_df.sort_values(by='警告数据数', ascending=False)
        
        # 创建柱状图
        try:
            # 筛选警告数据大于0的数据
            warning_chart_data = warning_df[warning_df['警告数据数'] > 0].copy()
            
            if not warning_chart_data.empty:
                # 创建图形
                fig, ax = plt.subplots(1, 1, figsize=(max(12, len(warning_chart_data) * 0.8), 8))
                
                # 绘制柱状图
                bars = ax.bar(range(len(warning_chart_data)), warning_chart_data['警告数据数'], 
                             color='orange', alpha=0.7, edgecolor='black')
                
                # 设置x轴标签
                ax.set_xticks(range(len(warning_chart_data)))
                ax.set_xticklabels(warning_chart_data['数据名称'], rotation=45, ha='right')
                
                # 设置标题和标签
                ax.set_title('测试项警告数据排名柱状图', fontsize=14, fontweight='bold')
                ax.set_xlabel('测试项名称')
                ax.set_ylabel('警告数据数量')
                
                # 在柱子上添加数值标签
                for i, bar in enumerate(bars):
                    height = bar.get_height()
                    ax.text(bar.get_x() + bar.get_width()/2., height + max(warning_chart_data['警告数据数']) * 0.01,
                           f'{int(height)}', ha='center', va='bottom', fontweight='bold')
                
                # 添加网格
                ax.grid(True, alpha=0.3, axis='y')
                
                # 调整布局
                plt.tight_layout()
                
                # 保存图片到输出文件夹
                # 生成图片文件名
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                chart_filename = f"警告数据排名柱状图_{timestamp}.png"
                chart_path = os.path.join(output_folder, chart_filename)
                
                # 保存图片
                plt.savefig(chart_path, format='png', dpi=300, bbox_inches='tight')
                plt.close()
                
                print("已保存警告数据柱状图")
            else:
                print("跳过警告数据柱状图")
                
        except Exception as e:
            print(f"生成警告数据柱状图失败: {e}")
        
        warning_df.to_excel(writer, sheet_name='测试项警告数据排名', index=False)

        # 超限数据排名
        outlimit_df = pd.DataFrame({
            '数据名称': [x['数据名称'] for x in col_stats_list],
            '超限数据数': [x['超限数据数'] for x in col_stats_list]
        })
        outlimit_df['超限数据排名'] = outlimit_df['超限数据数'].rank(method='min', ascending=False).astype(int)
        outlimit_df = outlimit_df.sort_values(by='超限数据数', ascending=False)
        
        # 创建超限数据柱状图
        try:
            # 筛选超限数据大于0的数据
            outlimit_chart_data = outlimit_df[outlimit_df['超限数据数'] > 0].copy()
            
            if not outlimit_chart_data.empty:
                # 创建图形
                fig, ax = plt.subplots(1, 1, figsize=(max(12, len(outlimit_chart_data) * 0.8), 8))
                
                # 绘制柱状图
                bars = ax.bar(range(len(outlimit_chart_data)), outlimit_chart_data['超限数据数'], 
                             color='red', alpha=0.7, edgecolor='black')
                
                # 设置x轴标签
                ax.set_xticks(range(len(outlimit_chart_data)))
                ax.set_xticklabels(outlimit_chart_data['数据名称'], rotation=45, ha='right')
                
                # 设置标题和标签
                ax.set_title('测试项超限数据排名柱状图', fontsize=14, fontweight='bold')
                ax.set_xlabel('测试项名称')
                ax.set_ylabel('超限数据数量')
                
                # 在柱子上添加数值标签
                for i, bar in enumerate(bars):
                    height = bar.get_height()
                    ax.text(bar.get_x() + bar.get_width()/2., height + max(outlimit_chart_data['超限数据数']) * 0.01,
                           f'{int(height)}', ha='center', va='bottom', fontweight='bold')
                
                # 添加网格
                ax.grid(True, alpha=0.3, axis='y')
                
                # 调整布局
                plt.tight_layout()
                
                # 保存图片到输出文件夹
                # 生成图片文件名
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                chart_filename = f"超限数据排名柱状图_{timestamp}.png"
                chart_path = os.path.join(output_folder, chart_filename)
                
                # 保存图片
                plt.savefig(chart_path, format='png', dpi=300, bbox_inches='tight')
                plt.close()
                
                print("已保存超限数据柱状图")
            else:
                print("跳过超限数据柱状图")
                
        except Exception as e:
            print(f"生成超限数据柱状图失败: {e}")
        
        outlimit_df.to_excel(writer, sheet_name='测试项超限数据排名', index=False)

        # 总数排名
        total_df = pd.DataFrame({
            '数据名称': [x['数据名称'] for x in col_stats_list],
            '总数': [x['总数'] for x in col_stats_list]
        })
        total_df['总数排名'] = total_df['总数'].rank(method='min', ascending=False).astype(int)
        total_df = total_df.sort_values(by='总数', ascending=False)
        
        # 创建总数柱状图
        try:
            # 筛选总数大于0的数据
            total_chart_data = total_df[total_df['总数'] > 0].copy()
            
            if not total_chart_data.empty:
                # 创建图形
                fig, ax = plt.subplots(1, 1, figsize=(max(12, len(total_chart_data) * 0.8), 8))
                
                # 绘制柱状图
                bars = ax.bar(range(len(total_chart_data)), total_chart_data['总数'], 
                             color='purple', alpha=0.7, edgecolor='black')
                
                # 设置x轴标签
                ax.set_xticks(range(len(total_chart_data)))
                ax.set_xticklabels(total_chart_data['数据名称'], rotation=45, ha='right')
                
                # 设置标题和标签
                ax.set_title('测试项总数排名柱状图', fontsize=14, fontweight='bold')
                ax.set_xlabel('测试项名称')
                ax.set_ylabel('总问题数据数量')
                
                # 在柱子上添加数值标签
                for i, bar in enumerate(bars):
                    height = bar.get_height()
                    ax.text(bar.get_x() + bar.get_width()/2., height + max(total_chart_data['总数']) * 0.01,
                           f'{int(height)}', ha='center', va='bottom', fontweight='bold')
                
                # 添加网格
                ax.grid(True, alpha=0.3, axis='y')
                
                # 调整布局
                plt.tight_layout()
                
                # 保存图片到输出文件夹
                # 生成图片文件名
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                chart_filename = f"总数排名柱状图_{timestamp}.png"
                chart_path = os.path.join(output_folder, chart_filename)
                
                # 保存图片
                plt.savefig(chart_path, format='png', dpi=300, bbox_inches='tight')
                plt.close()
                
                print("已保存总数柱状图")
            else:
                print("跳过总数柱状图")
                
        except Exception as e:
            print(f"生成总数柱状图失败: {e}")
        
        total_df.to_excel(writer, sheet_name='测试项总数排名', index=False)

        # 创建合并的排名柱状图
        top10_items = None  # 初始化变量
        try:
            # 筛选有问题的测试项（总数大于0）
            problem_data = total_df[total_df['总数'] > 0].copy()
            
            if not problem_data.empty:
                # 创建图形
                fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(max(12, len(problem_data) * 0.8), 15))
                
                # 获取对应的警告和超限数据
                warning_data = warning_df[warning_df['数据名称'].isin(problem_data['数据名称'])]
                outlimit_data = outlimit_df[outlimit_df['数据名称'].isin(problem_data['数据名称'])]
                
                # 按数据名称排序，确保三个图表的数据顺序一致
                problem_data = problem_data.sort_values(by='总数', ascending=False)
                warning_data = warning_data.set_index('数据名称').reindex(problem_data['数据名称']).reset_index()
                outlimit_data = outlimit_data.set_index('数据名称').reindex(problem_data['数据名称']).reset_index()
                
                # 绘制警告数据柱状图
                bars1 = ax1.bar(range(len(warning_data)), warning_data['警告数据数'], 
                               color='orange', alpha=0.7, edgecolor='black')
                ax1.set_title('测试项警告数据排名', fontsize=12, fontweight='bold')
                ax1.set_ylabel('警告数据数量')
                ax1.grid(True, alpha=0.3, axis='y')
                
                # 绘制超限数据柱状图
                bars2 = ax2.bar(range(len(outlimit_data)), outlimit_data['超限数据数'], 
                               color='red', alpha=0.7, edgecolor='black')
                ax2.set_title('测试项超限数据排名', fontsize=12, fontweight='bold')
                ax2.set_ylabel('超限数据数量')
                ax2.grid(True, alpha=0.3, axis='y')
                
                # 绘制总数柱状图
                bars3 = ax3.bar(range(len(problem_data)), problem_data['总数'], 
                               color='purple', alpha=0.7, edgecolor='black')
                ax3.set_title('测试项总数排名', fontsize=12, fontweight='bold')
                ax3.set_xlabel('测试项名称')
                ax3.set_ylabel('总问题数据数量')
                ax3.grid(True, alpha=0.3, axis='y')
                
                # 设置x轴标签（只在最后一个子图中显示）
                ax3.set_xticks(range(len(problem_data)))
                ax3.set_xticklabels(problem_data['数据名称'], rotation=45, ha='right')
                
                # 在柱子上添加数值标签
                for bars, ax in [(bars1, ax1), (bars2, ax2), (bars3, ax3)]:
                    for bar in bars:
                        height = bar.get_height()
                        if height > 0:
                            ax.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                                   f'{int(height)}', ha='center', va='bottom', fontweight='bold')
                
                # 调整布局
                plt.tight_layout()
                
                # 保存合并的柱状图
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                combined_chart_filename = f"测试项排名合并柱状图_{timestamp}.png"
                combined_chart_path = os.path.join(output_folder, combined_chart_filename)
                
                plt.savefig(combined_chart_path, format='png', dpi=300, bbox_inches='tight')
                plt.close()
                
                print("已保存合并柱状图")
                
                # 记录前十名信息，供后续使用
                top10_items = problem_data.head(10)
                create_excel_report.top10_items = top10_items
                
            else:
                print("跳过合并柱状图")
                
        except Exception as e:
            print(f"生成合并柱状图失败: {e}")
            top10_items = None  # 确保变量被定义


        # Sheet5: 复测率统计
        """
        retest_summary = pd.DataFrame({
            '统计项': ['总SN数量', '复测SN数量', '复测率'],
            '数值': [total_sn, retest_sn_count, f'{retest_rate:.2%}']
        })
        retest_summary.to_excel(writer, sheet_name='复测率统计', index=False)
        # 优化：复测SN明细sheet
        if not retest_df.empty:
            retest_df['复测排名'] = retest_df['复测次数'].rank(method='min', ascending=False).astype(int)
            retest_df = retest_df.sort_values(by='复测次数', ascending=False)
            # 强制所有SN号为字符串，防止Excel自动转数字
            retest_df['复测SN条码'] = retest_df['复测SN条码'].astype(str)
            # 写入Excel时设置列格式为文本
            for col in retest_df.columns:
                retest_df[col] = retest_df[col].astype(str) if col == '复测SN条码' else retest_df[col]
        else:
            retest_df = pd.DataFrame(columns=['复测SN条码', '复测次数', '复测排名'])
        # 使用 openpyxl 写入文本格式
        retest_df.to_excel(writer, sheet_name='复测SN明细', index=False)
        """

    print("Excel报告生成完成")
    return excel_path

def output_retest_txt(raw_data, output_folder):
    sn_list = raw_data.iloc[:, 0].dropna().astype(str).tolist()
    sn_seen = {}
    sn_retest = {}
    for sn in sn_list:
        if sn in sn_seen:
            sn_retest[sn] = sn_retest.get(sn, 1) + 1
        else:
            sn_seen[sn] = True
    retest_sn_counts = {sn: count for sn, count in sn_retest.items() if count > 1}
    total_sn = len(sn_list)
    retest_sn_count = len(retest_sn_counts)
    retest_rate = retest_sn_count / total_sn if total_sn > 0 else 0
    if retest_sn_counts:
        retest_df = pd.DataFrame({
            '复测SN条码': [str(sn) for sn in retest_sn_counts.keys()],
            '复测次数': list(retest_sn_counts.values())
        })
        retest_df['复测排名'] = retest_df['复测次数'].rank(method='min', ascending=False).astype(int)
        retest_df = retest_df.sort_values(by='复测次数', ascending=False)
        retest_df['复测SN条码'] = retest_df['复测SN条码'].astype(str)
    else:
        retest_df = pd.DataFrame(columns=['复测SN条码', '复测次数', '复测排名'])
    report_lines = []
    report_lines.append("复测率统计\n")
    report_lines.append(f"总SN数量: {total_sn}\n")
    report_lines.append(f"复测SN数量: {retest_sn_count}\n")
    report_lines.append(f"复测率: {retest_rate:.2%}\n\n")
    report_lines.append("复测SN明细:\n")
    if not retest_df.empty:
        for idx, row in retest_df.iterrows():
            report_lines.append(f"SN: {row['复测SN条码']}, 次数: {row['复测次数']}, 排名: {row['复测排名']}\n")
    else:
        report_lines.append("无复测SN数据\n")
    txt_filename = f"复测率统计_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    txt_path = os.path.join(output_folder, txt_filename)
    with open(txt_path, 'w', encoding='utf-8') as f:
        f.writelines(report_lines)
    print(f"复测率统计TXT已生成: {txt_path}")
    return txt_path

def plot_combined_distribution(stats_dict, data_info, lower_limits, upper_limits, output_folder):
    """绘制正态分布曲线和直方图的组合图（完全保持plot_analysis_v5的功能）"""
    import os
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    total_items = len(stats_dict)
    current = 0
    for col, stats_data in stats_dict.items():
        # 跳过排除的列
        if col in EXCLUDE_COLS:
            continue
        current += 1
        print(f"正在生成图表: {current}/{total_items}", end='\r')
        # 创建图形
        fig, ax = plt.subplots(1, 1, figsize=(12, 8))
        data_name = data_info[col]
        lower_limit = lower_limits.get(col, None)
        upper_limit = upper_limits.get(col, None)
        try:
            lower_limit = float(lower_limit) if pd.notna(lower_limit) else None
        except Exception:
            lower_limit = None
        try:
            upper_limit = float(upper_limit) if pd.notna(upper_limit) else None
        except Exception:
            upper_limit = None
        mean_val = stats_data['mean']
        std_val = stats_data['std']
        data = stats_data['data']
        if std_val <= 0 or np.isnan(std_val) or np.isinf(std_val):
            plt.close()
            continue
        x_min = min(data.min(), mean_val - 4 * std_val)
        x_max = max(data.max(), mean_val + 4 * std_val)
        if x_min >= x_max:
            x_min = mean_val - std_val
            x_max = mean_val + std_val
        x = np.linspace(x_min, x_max, 1000)
        try:
            y = stats.norm.pdf(x, mean_val, std_val)
            if np.any(np.isnan(y)) or np.any(np.isinf(y)):
                plt.close()
                continue
        except Exception:
            plt.close()
            continue
        try:
            n, bins, patches = ax.hist(data, bins=20, alpha=0.3, color='lightblue', edgecolor='black', density=True, label='数据分布')
            hist_counts, _ = np.histogram(data, bins=20)
            bin_centers = (bins[:-1] + bins[1:]) / 2
            for i, count in enumerate(hist_counts):
                if count > 0:
                    ax.text(bin_centers[i], hist_counts[i] / len(data) / (bins[1] - bins[0]) + max(y) * 0.01, f'{count}', ha='center', va='bottom', fontsize=8, color='darkblue')
        except Exception:
            plt.close()
            continue
        ax.plot(x, y, 'r-', linewidth=2, label=f'正态分布 (μ={mean_val:.2f}, σ={std_val:.2f})')
        warning_data = pd.Series([], dtype='float64')
        out_of_limit_data = pd.Series([], dtype='float64')
        if pd.notna(lower_limit) and pd.notna(upper_limit) and upper_limit > lower_limit:
            range_total = upper_limit - lower_limit
            if range_total > 0:
                warning_range = 0.05 * range_total
                warning_low_end = lower_limit + warning_range
                warning_high_start = upper_limit - warning_range
                if warning_low_end < warning_high_start:
                    warning_data = data[((data >= lower_limit) & (data <= warning_low_end)) | ((data >= warning_high_start) & (data <= upper_limit))]
                    out_of_limit_data = data[(data < lower_limit) | (data > upper_limit)]
        normal_data = data[~((data.index.isin(warning_data.index)) | (data.index.isin(out_of_limit_data.index)))]
        if not warning_data.empty:
            y_offset = np.random.uniform(0, max(y)*0.05, size=len(warning_data))
            ax.scatter(warning_data, y_offset, color='orange', marker='^', s=50, alpha=0.7, label='警告数据')
        if not out_of_limit_data.empty:
            y_offset = np.random.uniform(0, max(y)*0.05, size=len(out_of_limit_data))
            ax.scatter(out_of_limit_data, y_offset, color='red', marker='*', s=50, alpha=0.7, label='超限数据')
        if pd.notna(lower_limit):
            ax.axvline(x=lower_limit, color='red', linestyle='-', linewidth=2)
            ax.text(lower_limit, max(y)*0.5, f'下限: {lower_limit}', rotation=90, verticalalignment='center', bbox=dict(facecolor='white', alpha=0.5))
        if pd.notna(upper_limit):
            ax.axvline(x=upper_limit, color='red', linestyle='-', linewidth=2)
            ax.text(upper_limit, max(y)*0.5, f'上限: {upper_limit}', rotation=90, verticalalignment='center', bbox=dict(facecolor='white', alpha=0.5))
        if pd.notna(lower_limit) and pd.notna(upper_limit) and upper_limit > lower_limit:
            range_total = upper_limit - lower_limit
            if range_total > 0:
                warning_range = 0.05 * range_total
                warning_low_end = lower_limit + warning_range
                warning_high_start = upper_limit - warning_range
                if warning_low_end < warning_high_start:
                    ax.axvline(x=warning_low_end, color='yellow', linestyle='--', linewidth=1.5)
                    ax.text(warning_low_end, max(y)*0.5, f'警告下限: {warning_low_end:.2f}', rotation=90, verticalalignment='center', bbox=dict(facecolor='white', alpha=0.5))
                    ax.axvline(x=warning_high_start, color='yellow', linestyle='--', linewidth=1.5)
                    ax.text(warning_high_start, max(y)*0.5, f'警告上限: {warning_high_start:.2f}', rotation=90, verticalalignment='center', bbox=dict(facecolor='white', alpha=0.5))
        ax.set_xlabel('数值')
        ax.set_ylabel('概率密度')
        ax.set_title(f'{data_name}')
        ax.legend()
        ax.grid(True, alpha=0.3)
        plt.tight_layout()
        image_filename = f"{data_name}.png"
        image_path = os.path.join(output_folder, image_filename)
        plt.savefig(image_path, dpi=300, bbox_inches='tight')
        plt.close()
    print(f"全部图表生成完成，共{total_items}项。")

def get_input_file():
    """获取用户指定的输入文件"""
    if len(sys.argv) > 1:
        # 从命令行参数获取文件名
        input_file = sys.argv[1]
    else:
        # 交互式输入文件名
        print("请输入Excel文件名（例如：data.xlsx）：")
        input_file = input().strip()
        
        # 如果用户只输入了文件名，添加.xlsx后缀
        if not input_file.endswith(('.xlsx', '.xls')):
            input_file += '.xlsx'
    
    return input_file

def main():
    """主函数：集成筛选、分析和Excel报告功能"""
    import os
    
    # 获取用户指定的输入文件
    input_file = get_input_file()
    
    # 检查文件是否存在
    if not os.path.exists(input_file):
        print(f"错误：找不到文件 {input_file}")
        print("请确保文件在当前目录中，或提供完整的文件路径。")
        return
    
    try:
        print(f"\n{'='*60}")
        print("开始综合数据分析流程 (V2版本)...")
        print(f"{'='*60}")
        
        # 读取并筛选Excel文件
        print(f"\n步骤1: 读取并筛选Excel文件")
        print(f"正在读取文件: {input_file}")
        data_info, lower_limits, upper_limits, raw_data, valid_columns, original_df = read_and_filter_excel_data(input_file)
        
        if data_info is None:
            print("读取文件失败")
            return
        
        print(f"数据形状: {raw_data.shape}")
        
        # 处理数据
        print("步骤2: 数据处理...")
        processed_data = process_data(raw_data)
        
        if not processed_data:
            print("错误: 没有找到有效的数据列")
            return
        
        # 计算统计信息
        print("步骤3: 统计分析...")
        stats_dict = calculate_statistics(processed_data)
        
        # 分析数据质量
        print("步骤4: 质量分析...")
        warning_records, out_of_limit_records = analyze_data_quality(
            stats_dict, data_info, lower_limits, upper_limits, raw_data, original_df
        )
        
        print(f"问题数据: 警告{len(warning_records)}条, 超限{len(out_of_limit_records)}条")
        
        # 生成输出文件夹名，包含输入文件名、功能描述和运行时间
        base_name = os.path.splitext(os.path.basename(input_file))[0]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_folder = f"Report_{base_name}_综合分析_{timestamp}"  # 如 Report_2513FCT_综合分析_20250815_153000
        
        # 生成报告和图表
        print("步骤5: 生成报告和图表...")
        excel_path = create_excel_report(warning_records, out_of_limit_records, stats_dict, data_info, output_folder, raw_data)
        plot_combined_distribution(stats_dict, data_info, lower_limits, upper_limits, output_folder)
        # 复测率统计TXT输出（调用barcode_retest_rate_calc逻辑）
        output_retest_txt(raw_data, output_folder)
        # 所有测试项图像汇总（按总数排名重命名）
        print("步骤6: 创建测试项图像汇总...")
        import shutil
        image_folder = os.path.join(output_folder, "测试项图像汇总")
        if not os.path.exists(image_folder):
            os.makedirs(image_folder)
        # 获取总数排名信息
        col_stats = {}
        for col, stats_data in stats_dict.items():
            data_name = data_info.get(col, col)
            col_stats[col] = {
                '数据名称': data_name,
                '总数': 0
            }
        # 统计异常总数
        for rec in warning_records:
            col = rec.get('列名', None)
            if col in col_stats:
                col_stats[col]['总数'] = col_stats[col].get('总数', 0) + 1
        for rec in out_of_limit_records:
            col = rec.get('列名', None)
            if col in col_stats:
                col_stats[col]['总数'] = col_stats[col].get('总数', 0) + 1
        col_stats_list = list(col_stats.values())
        # 按总数降序排名
        ranked_df = pd.DataFrame(col_stats_list)
        ranked_df['总数排名'] = ranked_df['总数'].rank(method='min', ascending=False).astype(int)
        ranked_df = ranked_df.sort_values(by='总数', ascending=False)
        copied_count = 0
        for idx, row in ranked_df.iterrows():
            data_name = row['数据名称']
            rank = row['总数排名']
            total_count = row['总数']
            original_image_name = f"{data_name}.png"
            original_image_path = os.path.join(output_folder, original_image_name)
            if os.path.exists(original_image_path):
                ranked_image_name = f"第{rank:02d}名_{data_name}_总数{total_count}.png"
                ranked_image_path = os.path.join(image_folder, ranked_image_name)
                shutil.move(original_image_path, ranked_image_path)
                copied_count += 1
        print(f"测试项图像汇总完成: {copied_count}个文件")
        
        print(f"\n{'='*50}")
        print("分析完成！")
        print(f"输出文件夹: {output_folder}")
        print(f"处理列数: {len(valid_columns)}")
        print(f"问题数据: 警告{len(warning_records)}条, 超限{len(out_of_limit_records)}条")
        print(f"{'='*50}")
        
    except Exception as e:
        print(f"处理文件时发生错误: {str(e)}")

if __name__ == "__main__":
    main()