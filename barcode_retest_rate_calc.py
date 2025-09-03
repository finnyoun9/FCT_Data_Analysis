import pandas as pd
import os
import argparse
from datetime import datetime

def analyze_sn(file_path):
    # 跳过前4行，读取数据
    df = pd.read_excel(file_path, skiprows=4)
    sn_list = df.iloc[:, 0].dropna().astype(str).tolist()
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

    # 构建复测SN明细 DataFrame
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

    # 构建报告内容
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

    # 保存报告
    report_name = f"复测率统计_{os.path.splitext(os.path.basename(file_path))[0]}.txt"
    report_path = os.path.join(os.path.dirname(file_path), report_name)
    with open(report_path, 'w', encoding='utf-8') as f:
        f.writelines(report_lines)
    print(f"统计报告已生成: {report_path}")
    return report_path

def main():
    # 创建参数解析器
    parser = argparse.ArgumentParser(description='条码复测率统计分析工具')
    
    # 必选参数：输入文件
    parser.add_argument('input_file', help='要处理的Excel文件名')
    
    # 可选参数：输出目录
    parser.add_argument('-o', '--output-dir', help='报告输出目录，默认为输入文件所在目录')
    
    # 解析命令行参数
    args = parser.parse_args()
    
    # 检查输入文件是否存在
    if not os.path.exists(args.input_file):
        print(f"错误：找不到文件 {args.input_file}")
        return
    
    try:
        # 调用分析函数
        report_path = analyze_sn(args.input_file)
        
        # 如果指定了输出目录，则移动报告文件
        if args.output_dir:
            os.makedirs(args.output_dir, exist_ok=True)
            new_report_path = os.path.join(args.output_dir, os.path.basename(report_path))
            os.rename(report_path, new_report_path)
            print(f"报告已移动到: {new_report_path}")
        
    except Exception as e:
        print(f"处理文件时发生错误: {str(e)}")

if __name__ == "__main__":
    main()