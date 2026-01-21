import pandas as pd
import datetime
from emailUtils import connect_mail, mark_email_as_read, select_mail, search_mail, process_email, send_email_with_attachment
import os
import sys

# 将上一级目录添加到Python搜索路径
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

# 现在可以导入importFeishu模块
import save_to_feishu_sdk, build_app, feishu_fields

# 配置参数
address = '819147806@qq.com'
password = 'purzjkococqlbbha'
server = 'imap.qq.com'
port = 993
subject = '鹰眼全网监测主题报'
from_address = 'eageyealarm@eefung.com'  # eageyealarm@eefung.com

# 配置SMTP参数（使用QQ邮箱的SMTP服务器）
smtp_server = 'smtp.qq.com'
smtp_port = 465
to_email = 'notifier@chinasatnet.com.cn' 
# to_email = 'datasystem2025@163.com'  # 收件人邮箱

gw_target_columns = [
        '信源类型', '发布时间', '文章链接地址', '网站名称',
        '标题', '标题(译文)',
        '正文', '正文(译文)',
        '匹配词',
        '摘要', '摘要(译文)']


# 邮件表格列与飞书字段的映射关系
email_to_feishu_map = {
    '标题(译文)': '原文标题',
    '文章链接地址': '动态来源',
    '正文': '动态原文',
    '正文(译文)':'全文翻译'
}

gn_target_columns = ['信源类型','发布时间','公众号名称','标题','正文']


# 使原表格适应目标表格要求，根据境内/境外数据类型分别处理
def transform_table_columns(df, category):
    """
    根据数据类别转换表格列格式
    
    参数:
        df: 原始DataFrame
        category: 数据类别 (0: 境内数据, 1: 境外数据)
    
    返回:
        转换后的DataFrame
    """
    # 创建副本避免修改原数据
    new_df = df.copy()
    
    # 根据类别选择目标列
    target_columns = gw_target_columns if category == 1 else gn_target_columns
    
    # 处理境内数据的特殊映射：公众号名称对应网站名称
    if category == 0:
        new_df = new_df.rename(columns={'作者名': '公众号名称'})
    
    # 添加缺失字段（保持原始数据不变）
    for col in target_columns:
        if col not in new_df.columns:
            new_df[col] = ''
    
    # 按目标顺序重新排列列
    return new_df[target_columns]

def map_email_data_to_feishu(df, category):
    """
    将邮件表格数据映射到飞书字段格式
    
    参数:
        df: 处理后的邮件表格DataFrame
        category: 数据类别 (0: 境内数据, 1: 境外数据)
    
    返回:
        飞书格式的数据列表 [{'字段1': 值1, '字段2': 值2, ...}, ...]
    """
    feishu_data = []
    
    # 只处理境外数据
    if category != 1:
        return feishu_data
    
    for index, row in df.iterrows():
        item = {}
        
        # 映射已知字段
        for email_col, feishu_col in email_to_feishu_map.items():
            if email_col in row:
                value = row[email_col]
                
                # 处理Link类型字段 - 动态来源
                if feishu_col == '动态来源':
                    if pd.notna(value) and value:
                        # Link类型需要对象格式，这里使用网站名称作为文本
                        item[feishu_col] = {
                            "text": str(value),
                            "link": ""
                        }
                    else:
                        item[feishu_col] = None
                # 普通字段直接赋值
                else:
                    item[feishu_col] = value
        
        # 设置固定字段
        item['提报日期'] = None
        item['内容类型'] = '动态'
        # 设置默认的审核人文本字段
        item['审核人文本'] = '王林宝'
        
        # 为缺失的飞数字段设置默认值，跳过提报人字段
        for field in feishu_fields:
            if field not in item:
                if field != '提报人 (人员 )':
                    item[field] = None
        
        feishu_data.append(item)
    
    return feishu_data


# 修改主程序逻辑，为每个邮件单独生成表格
if __name__ == '__main__':
    
    # 登陆邮箱
    mail, login_state = connect_mail(server, port, address, password)
    print('1.执行邮箱登陆：')
    if login_state == 'AUTH':
        print('【成功】')
    else:
        print('【登陆失败，请检查邮箱和密码】')
        exit()

    # 选择收件箱
    select_status, msgs = select_mail(mail)
    print('2.执行选中收件箱：')
    if select_status == 'OK':
        print('【成功】')
    else:
        print('【失败】:', msgs)

    # 搜索来自指定发件箱的邮件
    search_status, messages = search_mail(mail, from_address)
    if search_status == 'OK':
        email_ids = messages[0].split()
        print('3.查找未读邮件：')
        if not email_ids:
            print('【无未读邮件】')
            exit()
        else:
            print(f'【找到 {len(email_ids)} 封未读邮件】')
            print('4.处理邮件附件')
            # 处理所有邮件，为每个邮件单独生成表格
            for email_id in email_ids:
                # 处理单封邮件
                print('=======读取邮件========')
                print('4-1 邮件解析:')
                email_data, status_msg = process_email(mail, email_id)
                print(f"\n{status_msg}")
                if email_data:
                    print(f"邮件标题: {email_data['subject']}")
                    print(f"发件人: {email_data['from_email']}")
                    print(f"收件时间: {email_data['received_date']}")
                    category = email_data['category']
                    if category == 1: print('识别为境外数据')
                    elif category == 0: print('识别为境内数据')
                    
                    if email_data['tables']:
                        print(f"找到 {len(email_data['tables'])} 个表格附件")
                        # 为当前邮件创建一个DataFrame
                        email_df = pd.DataFrame()
                        
                        for table in email_data['tables']:
                            df = table['dataframe']
                            # 转换为目标格式
                            df = transform_table_columns(df, category)
                            
                            # 将当前表格添加到邮件的DataFrame中
                            if email_df.empty:
                                email_df = df.copy()
                            else:
                                email_df = pd.concat([email_df, df], ignore_index=True)
                        print('4-3 表格格式转换完成')
                    
                    # 在保存文件的部分修改并添加发送邮件功能
                        if not email_df.empty:
                            if category == 1: email_df['信源类型'] = '境外媒体'
                            elif category == 0: email_df['信源类型'] = '微信'
                            email_df['发布时间'] = pd.to_datetime(email_df['发布时间'], format='%Y-%m-%d %H:%M:%S').astype(str)
                            
                            # 使用当前文件所在目录的绝对路径
                            output_dir = os.path.join(os.path.dirname(__file__), 'output')
                            
                            # 确保output目录存在
                            os.makedirs(output_dir, exist_ok=True)
                            
                            # 构建完整的文件路径
                            category_text = "境外" if category == 1 else "境内"
                            output_file = os.path.join(output_dir, f"{email_data['time_slot']}{category_text}数据.xlsx")
                            
                            # 保存当前邮件的表格
                            email_df.to_excel(output_file, index=False)
                            print(f"4-4 邮件表格保存到本地")
                        
                            
                            # 同步境外数据到飞书
                            if category == 1:
                                print('4-6 同步境外数据到飞书：')
                                # 映射邮件数据到飞书格式
                                feishu_data = map_email_data_to_feishu(email_df, category)
                                if feishu_data:
                                    print(f"准备同步 {len(feishu_data)} 条境外数据到飞书...")
                                    # 构建飞书客户端
                                    client = build_app()
                                    # 保存到飞书
                                    result = save_to_feishu_sdk(client, feishu_data)
                                    print('【飞书同步完成】')
                                else:
                                    print('【没有需要同步的境外数据】')
                            
                            # 将邮件标记为已读
                            # if send_status:
                            #     mark_email_as_read(mail,email_id)

        print("\n=====所有邮件处理完成======")
    else:
        print("未找到任何表格数据")
else:
    print('搜索失败:', messages)