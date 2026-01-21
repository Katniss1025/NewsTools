import lark_oapi as lark
import json
import logging
import difflib
from typing import Dict, List, Any, Optional, Union
from lark_oapi.api.bitable.v1 import *
from lark_oapi.api.contact.v3 import *

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger(__name__)


class FeishuConfig:
    """飞书配置类"""
    def __init__(self):
        self.APP_ID: str = "cli_a9eee21b86385bc6"
        self.APP_SECRET: str = "ZFYwQvF6PPHOCSJdpWrPmfHg1a6Pn1fm"
        self.APP_TOKEN: str = "SdSlbKV5iaACjBsCVwNcJ6e8nYc"
        self.GW_TABLE_ID: str = "tblQ4sWd6zj5iFfC"
        self.GN_TABLE_ID: str = "tblcgYb7wnhvN11F"
        self.DUPLICATED_TABLE_ID: str = "tblwCE4E6QiS7373"


class FeishuFields:
    """飞书字段定义类"""
    # 动态相关字段
    NEWS_TITLE: str = "原文标题"
    NEWS_CONTENT: str = "动态原文"
    NEWS_SOURCE: str = "动态来源"
    
    # 人员相关字段
    REVIEWER_TEXT: str = "审核人文本"
    
    # 分类与标签字段
    NEWS_CATEGORY: str = "内容类型"
    
    # 所有字段列表
    ALL_FIELDS: List[str] = [
        NEWS_TITLE,
        NEWS_CONTENT,
        NEWS_SOURCE,
        NEWS_CATEGORY,
        REVIEWER_TEXT,
    ]


# 创建配置实例
feishu_config = FeishuConfig()
feishu_fields = FeishuFields.ALL_FIELDS


def build_feishu_client() -> lark.Client:
    """
    创建并返回飞书客户端实例
    
    Returns:
        lark.Client: 飞书客户端实例
    """
    try:
        client = lark.Client.builder() \
            .app_id(feishu_config.APP_ID) \
            .app_secret(feishu_config.APP_SECRET) \
            .build()
        logger.info("飞书客户端创建成功")
        return client
    except Exception as e:
        logger.error(f"飞书客户端创建失败: {str(e)}")
        raise


class FeishuDataSaver:
    """飞书数据保存类"""
    
    def __init__(self, client: lark.Client):
        self.client = client
    
    def _validate_data(self, news_data: List[Dict[str, Any]]) -> bool:
        """
        验证数据格式
        
        Args:
            news_data: 要验证的数据列表
            
        Returns:
            bool: 数据是否有效
        """
        if not news_data:
            logger.warning("没有要保存的数据")
            return False
            
        for i, item in enumerate(news_data):
            if not isinstance(item, dict):
                logger.error(f"第 {i+1} 条数据不是字典格式: {item}")
                return False
                
        return True
    
    def _build_records(self, news_data: List[Dict[str, Any]]) -> List[AppTableRecord]:
        """
        构建飞书记录列表
        
        Args:
            news_data: 原始数据列表
            
        Returns:
            List[AppTableRecord]: 飞书记录列表
        """
        records = []
        for item in news_data:
            record = AppTableRecord.builder().fields(item).build()
            records.append(record)
        return records
    
    def _build_request(self, records: List[AppTableRecord], data_type: int) -> BatchCreateAppTableRecordRequest:
        """
        构建批量创建请求
        
        Args:
            records: 飞书记录列表
            data_type: 数据类型 (0: 境内数据, 1: 境外数据)
            
        Returns:
            BatchCreateAppTableRecordRequest: 批量创建请求
        """
        request_body = BatchCreateAppTableRecordRequestBody.builder().records(records).build()
        
        # 根据数据类型选择表格ID
        table_id = feishu_config.GN_TABLE_ID if data_type == 0 else feishu_config.GW_TABLE_ID
        
        request = BatchCreateAppTableRecordRequest.builder() \
            .app_token(feishu_config.APP_TOKEN) \
            .table_id(table_id) \
            .request_body(request_body) \
            .build()
        
        return request
    
    def deduplicate_by_title_similarity(self, items, threshold=0.7, existing_titles=None):
        """
        根据标题相似度进行去重
        
        Args:
            items: 待去重的数据列表
            threshold: 相似度阈值，默认为0.7
            existing_titles: 已存在的标题列表，用于与飞书已有记录去重
            
        Returns:
            tuple: (去重后的数据列表, 重复的数据列表)
        """
        if not items:
            return [], []
        
        # 移除标题为空的项
        valid_items = []
        for item in items:
            title = item.get('title')
            if title and str(title).strip():
                valid_items.append(item)
        
        if len(valid_items) <= 1:
            return valid_items, []
        
        to_keep = []
        duplicates = []
        processed_titles = []
        
        for item in valid_items:
            current_title = str(item['title']).strip()
            is_duplicate = False
            duplicate_title = None
            
            # 1. 检查是否与已处理的标题重复（邮件内去重）
            for processed_title in processed_titles:
                similarity = difflib.SequenceMatcher(None, current_title, processed_title).ratio()
                if similarity >= threshold:
                    is_duplicate = True
                    duplicate_title = processed_title
                    item['similarity'] = similarity
                    logger.info(f"【邮件内去重】发现重复标题: '{current_title}' 与 '{processed_title}' 相似度: {similarity:.2f}")
                    break
            
            # 2. 如果邮件内没有重复，检查是否与飞书已有记录重复
            if not is_duplicate and existing_titles:
                for existing_title in existing_titles:
                    existing_title_str = str(existing_title).strip()
                    similarity = difflib.SequenceMatcher(None, current_title, existing_title_str).ratio()
                    if similarity >= threshold:
                        is_duplicate = True
                        duplicate_title = existing_title_str
                        item['similarity'] = similarity
                        logger.info(f"【飞书记录去重】发现重复标题: '{current_title}' 与 '{existing_title_str}' 相似度: {similarity:.2f}")
                        break
            
            if not is_duplicate:
                to_keep.append(item)
                processed_titles.append(current_title)
            else:
                item['duplicate_title'] = duplicate_title
                duplicates.append(item)
        
        logger.info(f"【标题相似度去重完成】: 从 {len(valid_items)} 条记录中去重后保留 {len(to_keep)} 条，重复 {len(duplicates)} 条")
        return to_keep, duplicates
    
    def get_existing_records(self, data_type: int):
        """
        获取飞书表格中已有的记录
        
        Args:
            data_type: 数据类型 (0: 境内数据, 1: 境外数据)
            
        Returns:
            list: 已有的记录列表
        """
        try:
            # 根据数据类型选择表格ID
            table_id = feishu_config.GN_TABLE_ID if data_type == 0 else feishu_config.GW_TABLE_ID
            
            # 构建请求
            request = ListAppTableRecordRequest.builder()
            request = request.app_token(feishu_config.APP_TOKEN)
            request = request.table_id(table_id)
            request = request.page_size(1000)  # 设置较大的页面大小，确保获取所有记录
            request = request.build()
            
            # 发起请求
            response = self.client.bitable.v1.app_table_record.list(request)
            
            # 处理响应
            if response.success():
                records = response.data.items
                existing_records = []
                if records:
                    for record in records:
                        fields = record.fields
                        if isinstance(fields, dict) and FeishuFields.NEWS_TITLE in fields:
                            existing_records.append({
                                'title': fields[FeishuFields.NEWS_TITLE],
                                'record_id': record.record_id
                            })
                logger.info(f"✅ 获取飞书表格已有记录成功! 共 {len(existing_records)} 条记录")
                return existing_records
            else:
                logger.error(f"❌ 获取飞书表格已有记录失败, code: {response.code}, msg: {response.msg}")
                return []
        except Exception as e:
            logger.error(f"❌ 获取飞书表格已有记录过程中发生异常: {str(e)}")
            logger.exception("异常堆栈信息:")
            return []
    
    def save_duplicate_record(self, duplicate_info):
        """
        保存重复记录到DUPLICATED_TABLE_ID表格
        
        Args:
            duplicate_info: 重复记录信息，包含 "重复记录" 字段
            
        Returns:
            bool: 保存成功返回True，失败返回False
        """
        try:
            # 构建记录
            record_builder = AppTableRecord.builder()
            record_builder = record_builder.fields(duplicate_info)
            record = record_builder.build()
            
            # 构建请求体
            body_builder = BatchCreateAppTableRecordRequestBody.builder()
            body_builder = body_builder.records([record])
            request_body = body_builder.build()
            
            # 构建请求
            request_builder = BatchCreateAppTableRecordRequest.builder()
            request_builder = request_builder.app_token(feishu_config.APP_TOKEN)
            request_builder = request_builder.table_id(feishu_config.DUPLICATED_TABLE_ID)
            request_builder = request_builder.request_body(request_body)
            request = request_builder.build()
            
            # 发起请求
            response = self.client.bitable.v1.app_table_record.batch_create(request)
            
            # 处理响应
            if response.success():
                logger.info("✅ 保存重复记录成功!")
                return True
            else:
                logger.error(f"❌ 保存重复记录失败, code: {response.code}, msg: {response.msg}")
                return False
        except Exception as e:
            logger.error(f"❌ 保存重复记录过程中发生异常: {str(e)}")
            logger.exception("异常堆栈信息:")
            return False
    
    def save_data(self, news_data: List[Dict[str, Any]], data_type: int = 1) -> Optional[List[AppTableRecord]]:
        """
        保存数据到飞书多维表格
        
        Args:
            news_data: 要保存的数据列表
            data_type: 数据类型 (0: 境内数据, 1: 境外数据, 默认为境外数据)
            
        Returns:
            Optional[List[AppTableRecord]]: 创建的记录列表，失败时返回None
        """
        # 验证数据
        if not self._validate_data(news_data):
            return None
        
        try:
            # 构建记录
            records = self._build_records(news_data)
            
            # 构建请求
            request = self._build_request(records, data_type)
            
            # 发起请求
            response = self.client.bitable.v1.app_table_record.batch_create(request)
            
            # 处理响应
            if response.success():
                created_records = response.data.records
                logger.info(f"✅ 批量保存成功!")
                logger.info(f"   共处理 {len(news_data)} 条记录")
                logger.info(f"   成功创建 {len(created_records)} 条记录")
                
                # 输出每条记录的ID
                for i, record in enumerate(created_records):
                    logger.info(f"   记录 {i+1} - Record ID: {record.record_id}")
                
                return created_records
            else:
                logger.error(f"❌ 批量保存失败, code: {response.code}, msg: {response.msg}")
                
                # 输出详细错误信息
                try:
                    error_detail = json.loads(response.raw.content)
                    logger.error(f"   详细错误: {json.dumps(error_detail, indent=4, ensure_ascii=False)}")
                except Exception as e:
                    logger.error(f"   解析错误信息失败: {str(e)}")
                    logger.error(f"   原始错误响应: {response.raw.content}")
                
        except Exception as e:
            logger.error(f"❌ 保存过程中发生异常: {str(e)}")
            logger.exception("异常堆栈信息:")
        
        return None


def batch_save_data(client: lark.Client, news_data: Union[Dict[str, Any], List[Dict[str, Any]]], data_type: int = 1) -> Optional[List[AppTableRecord]]:
    """
    批量保存新闻数据到飞书多维表格
    
    Args:
        client: 飞书客户端实例
        news_data: 单条记录(dict)或多条记录(list of dict)
        data_type: 数据类型 (0: 境内数据, 1: 境外数据, 默认为境外数据)
        
    Returns:
        Optional[List[AppTableRecord]]: 创建的记录列表，失败时返回None
    """
    # 确保news_data是列表格式
    if isinstance(news_data, dict):
        news_data = [news_data]  # 转换为单元素列表
    
    # 创建数据保存器实例
    saver = FeishuDataSaver(client)
    
    # 保存数据
    return saver.save_data(news_data, data_type)


def get_field_name(field_enum: str) -> str:
    """
    获取字段的中文名称
    
    Args:
        field_enum: 字段枚举名称
        
    Returns:
        str: 字段中文名称
    """
    field_map = {
        # 动态相关字段
        "NEWS_TITLE": FeishuFields.NEWS_TITLE,
        "NEWS_SOURCE": FeishuFields.NEWS_SOURCE,
        "NEWS_CONTENT": FeishuFields.NEWS_CONTENT,
        
        # 人员相关字段
        "REVIEWER_TEXT": FeishuFields.REVIEWER_TEXT,
        
        # 分类与标签字段
        "NEWS_CATEGORY": FeishuFields.NEWS_CATEGORY,
    }
    return field_map.get(field_enum, field_enum)


def send_feishu_webhook_notification(total_updated: int, gn_updated: int, gw_updated: int, reviewer: str) -> bool:
    """发送飞书webhook通知
    
    Args:
        total_updated: 更新的记录总数
        gn_updated: 境内更新记录数
        gw_updated: 境外更新记录数
        reviewer: 审核人姓名
        
    Returns:
        bool: 发送成功返回True，失败返回False
    """
    import requests
    
    webhook_url = "https://open.feishu.cn/open-apis/bot/v2/hook/9d7f45c8-2214-4b0e-986d-878f2136bb73"
    message = f"今日动态更新{total_updated}条：境内{gn_updated}条，境外{gw_updated}条。请{reviewer}同学前往处理！"
    
    try:
        payload = {
            "msg_type": "text",
            "content": {
                "text": message
            }
        }
        
        response = requests.post(webhook_url, json=payload, headers={"Content-Type": "application/json"})
        response.raise_for_status()  # 检查请求是否成功
        logger.info(f"飞书webhook发送成功: {response.json()}")
        return True
    except Exception as e:
        logger.error(f"飞书webhook发送失败: {str(e)}")
        return False


if __name__ == "__main__":
    # 构建飞书客户端
    client = build_feishu_client()
    
    # 示例：批量保存多条记录到飞书多维表格
    logger.info("\n=== 演示批量保存多条记录 ===")
    
    # 准备多条测试数据
    batch_data = [
        {
            FeishuFields.NEWS_TITLE: "测试标题1",
            FeishuFields.NEWS_CONTENT: "测试内容1",
            FeishuFields.NEWS_CATEGORY: "动态",
            FeishuFields.REVIEWER_TEXT: "尹晓丹"
        },
        {
            FeishuFields.NEWS_TITLE: "测试标题2",
            FeishuFields.NEWS_CONTENT: "测试内容2",
            FeishuFields.NEWS_CATEGORY: "文章",
            FeishuFields.REVIEWER_TEXT: "尹晓丹"
        },
        {
            FeishuFields.NEWS_TITLE: "测试标题3",
            FeishuFields.NEWS_CONTENT: "测试内容3",
            FeishuFields.NEWS_CATEGORY: "动态",
            FeishuFields.REVIEWER_TEXT: "尹晓丹"
        }
    ]
    
    # 调用批量保存函数
    save_to_feishu_sdk(client, batch_data)