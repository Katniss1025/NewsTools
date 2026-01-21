import os
import sys
import logging
import pandas as pd

# 添加当前目录到系统路径，以便导入email_tools和feishu_tools
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 导入邮件相关类
from email_tools.email_reader import EmailConfig, DataConfig, EmailProcessor

# 添加项目根目录到系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 导入飞书相关类
from feishu_tools.save_data_to_feishu import FeishuDataSaver, FeishuConfig, FeishuFields, build_feishu_client, send_feishu_webhook_notification

# 配置日志
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger(__name__)




def main():
    """主函数"""
    try:
        logger.info("===== 开始处理新闻数据 =====")
        
        # 初始化配置和处理器
        email_config = EmailConfig()
        data_config = DataConfig()
        
        # 创建邮件处理器
        email_processor = EmailProcessor(email_config, data_config)
        
        # 初始化飞书配置和数据保存器
        feishu_client = build_feishu_client()
        feishu_saver = FeishuDataSaver(feishu_client)
        
        # 记录更新条数
        total_updated = 0
        gn_updated = 0  # 境内更新条数
        gw_updated = 0  # 境外更新条数
        
        # 登录邮箱
        if not email_processor.login():
            logger.error("邮箱登录失败，程序退出")
            return
        
        # 选择收件箱
        if not email_processor.select_inbox():
            logger.error("选择收件箱失败，程序退出")
            return
        
        # 搜索邮件
        email_ids = email_processor.search_emails()
        if not email_ids:
            logger.info("无未读邮件，程序退出")
            return
        
        # 处理所有邮件
        logger.info("开始处理邮件附件")
        
        for email_id in email_ids:
            logger.info(f"\n======= 处理邮件 ID: {email_id} =======")
            
            try:
                # 处理单封邮件
                email_data, status_msg = email_processor.process_email(email_id)
                logger.info(f"邮件解析状态: {status_msg}")
                
                if not email_data or not email_data['tables']:
                    logger.warning("邮件无有效表格数据，跳过")
                    continue
                
                # 处理表格数据
                items_to_process = []
                for table in email_data['tables']:
                    df = table['dataframe']
                    category = email_data['category'] # 识别境内数据0还是境外数据1
                    
                    # 转换表格格式
                    transformed_df = df.copy()
                    
                    # 设置信源类型
                    source_type = data_config.source_type_map.get(category, '未知')
                    transformed_df['信源类型'] = source_type
                    
                    # 将DataFrame转换为处理所需的格式
                    for _, row in transformed_df.iterrows():
                        # 动态来源
                        news_source_name = row.get('网站名称', '')
                        news_source_url = row.get('文章链接地址', '')
                        
                        # Link类型需要对象格式，且必须至少包含text或link中的一个
                        link_text = str(news_source_name) if pd.notna(news_source_name) and news_source_name else "未知来源"
                        link_url = str(news_source_url) if pd.notna(news_source_url) and news_source_url else ""
                        
                        news_source_link = {
                            "text": link_text,
                            "link": link_url
                        }
                        
                        # 固定审核人
                        reviewer = "尹晓丹"
                        
                        # 根据数据类型选择标题字段 (category: 0=境内, 1=境外)
                        if category == 0:
                            title_field = '标题'
                        else:
                            title_field = '标题(译文)'
                        
                        title = row.get(title_field, '')
                        
                        item = {
                            'title': title,
                            'category': category,
                            'record': {
                                FeishuFields.NEWS_TITLE: title,
                                FeishuFields.NEWS_CONTENT: row.get('正文', ''),
                                FeishuFields.NEWS_SOURCE: news_source_link,
                                # 内容类型默认为“动态”
                                FeishuFields.NEWS_CATEGORY: '动态',
                                FeishuFields.REVIEWER_TEXT: reviewer  # 固定审核人
                            }
                        }
                        items_to_process.append(item)
                
                # 1. 对邮件内的数据进行标题相似度去重
                unique_items, _ = feishu_saver.deduplicate_by_title_similarity(items_to_process,threshold=0.5)
                
                # 2. 获取飞书表格中已有的记录
                existing_records = feishu_saver.get_existing_records(category)
                existing_titles = [record['title'] for record in existing_records if 'title' in record]
                
                # 3. 与飞书已有记录进行标题相似度去重
                final_items, duplicate_items = feishu_saver.deduplicate_by_title_similarity(unique_items, existing_titles=existing_titles,threshold=0.5)
                
                # 4. 处理重复记录，保存到DUPLICATED_TABLE_ID表格
                for item in duplicate_items:
                    category_text = '境内' if item['category'] == 0 else '境外'
                    duplicate_info = {
                        '重复记录': f"{category_text}：{item['title']}（已有重复记录的标题：{item.get('duplicate_title', '')}）"
                    }
                    # 添加原文标题、动态原文、动态来源、审核人文本字段
                    if 'record' in item:
                        record = item['record']
                        if FeishuFields.NEWS_TITLE in record:
                            duplicate_info[FeishuFields.NEWS_TITLE] = record[FeishuFields.NEWS_TITLE]
                        if FeishuFields.NEWS_CONTENT in record:
                            duplicate_info[FeishuFields.NEWS_CONTENT] = record[FeishuFields.NEWS_CONTENT]
                        if FeishuFields.NEWS_SOURCE in record:
                            duplicate_info[FeishuFields.NEWS_SOURCE] = record[FeishuFields.NEWS_SOURCE]
                        if FeishuFields.REVIEWER_TEXT in record:
                            duplicate_info[FeishuFields.REVIEWER_TEXT] = record[FeishuFields.REVIEWER_TEXT]
                    # 添加相似度字段
                    if 'similarity' in item:
                        duplicate_info['相似度'] = str(round(item['similarity'], 2))
                    feishu_saver.save_duplicate_record(duplicate_info)
                
                # 5. 准备保存到飞书的数据
                processed_data = [item['record'] for item in final_items]
                
                # 保存数据到飞书
                if processed_data:
                    logger.info(f"准备保存 {len(processed_data)} 条记录到飞书")
                    # 根据数据类型选择表格 (category: 0=境内, 1=境外)
                    result = feishu_saver.save_data(processed_data, data_type=category)
                    
                    if result:
                        logger.info("数据保存到飞书成功")
                        # 更新总处理条数
                        total_updated += len(processed_data)
                        # 根据数据类型更新对应的计数器 (category: 0=境内, 1=境外)
                        if category == 0:
                            gn_updated += len(processed_data)
                        else:
                            gw_updated += len(processed_data)
                        # 标记邮件为已读
                        # logger.info("标记邮件为已读")
                        # email_processor.mark_email_as_read(email_id)
                    else:
                        logger.error("数据保存到飞书失败")
                else:
                    logger.warning("无有效数据可保存到飞书")
                
            except Exception as e:
                logger.error(f"处理邮件时发生异常: {str(e)}")
                continue
        
        logger.info(f"\n===== 所有新闻数据处理完成 =====")
        logger.info(f"总共更新了 {total_updated} 条记录")
        
        # 发送飞书webhook提醒
        reviewer = "尹晓丹"  # 固定审核人
        # send_feishu_webhook_notification(total_updated, gn_updated, gw_updated, reviewer)
        
    except Exception as e:
        logger.error(f"程序执行异常: {str(e)}")


if __name__ == '__main__':
    main()