import pandas as pd
from .emailUtils import connect_mail, select_mail, search_mail, process_email
import logging
import difflib
import os

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger(__name__)


class EmailConfig:
    """邮件配置类"""
    def __init__(self):
        # IMAP配置
        self.address = '819147806@qq.com'
        self.password = 'purzjkococqlbbha'
        self.server = 'imap.qq.com'
        self.port = 993
        
        # 邮件过滤配置
        self.subject = '鹰眼全网监测主题报'
        self.from_address = 'eageyealarm@eefung.com'
        
        # SMTP配置
        self.smtp_server = 'smtp.qq.com'
        self.smtp_port = 465
        self.to_email = 'notifier@chinasatnet.com.cn'
        # self.to_email = 'datasystem2025@163.com'  # 测试收件人邮箱


class DataConfig:
    """数据配置类"""
    def __init__(self):
        # 境外数据目标列
        self.gw_target_columns = [
            '信源类型', '发布时间', 
            '文章链接地址', '网站名称',
            '标题', '标题(译文)',
            '正文', '正文(译文)',
            '匹配词', '摘要', '摘要(译文)'
        ]
        
        # 境内数据目标列
        self.gn_target_columns = [
            '信源类型', '发布时间', '公众号名称', '标题', '正文'
        ]
        
        # 数据类别映射
        self.category_map = {
            0: '境内',
            1: '境外'
        }
        
        # 信源类型映射
        self.source_type_map = {
            0: '微信',
            1: '境外媒体'
        }


class EmailProcessor:
    """邮件处理器"""
    
    def __init__(self, email_config: EmailConfig, data_config: DataConfig):
        self.email_config = email_config
        self.data_config = data_config
        self.mail = None
        
    def login(self):
        """登录邮箱"""
        logger.info("1. 执行邮箱登陆")
        try:
            self.mail, login_state = connect_mail(
                self.email_config.server,
                self.email_config.port,
                self.email_config.address,
                self.email_config.password
            )
            
            if login_state == 'AUTH':
                logger.info("【成功】")
                return True
            else:
                logger.error("【登陆失败，请检查邮箱和密码】")
                return False
        except Exception as e:
            logger.error(f"【登陆异常】: {str(e)}")
            return False
    
    def select_inbox(self):
        """选择收件箱"""
        logger.info("2. 执行选中收件箱")
        try:
            select_status, msgs = select_mail(self.mail)
            if select_status == 'OK':
                logger.info("【成功】")
                return True
            else:
                logger.error(f"【失败】: {msgs}")
                return False
        except Exception as e:
            logger.error(f"【选择收件箱异常】: {str(e)}")
            return False
    
    def search_emails(self):
        """搜索指定发件人的邮件"""
        logger.info("3. 查找未读邮件")
        try:
            search_status, messages = search_mail(self.mail, self.email_config.from_address)
            if search_status == 'OK':
                email_ids = messages[0].split() if messages and messages[0] else []
                logger.info(f"【找到 {len(email_ids)} 封未读邮件】")
                return email_ids
            else:
                logger.error("【搜索失败】")
                return []
        except Exception as e:
            logger.error(f"【搜索邮件异常】: {str(e)}")
            return []
    
    def deduplicate_by_title_similarity(self, df, title_column='标题', threshold=0.8):
        """
        根据标题相似度去重
        
        参数:
            df: 原始DataFrame
            title_column: 标题列名
            threshold: 相似度阈值，超过此阈值认为是重复
        
        返回:
            去重后的DataFrame
        """
        try:
            if df.empty or title_column not in df.columns:
                return df
            
            # 移除标题为空的行
            df = df.dropna(subset=[title_column])
            df = df[df[title_column].astype(str).str.strip() != '']
            
            if len(df) <= 1:
                return df
            
            # 记录需要保留的行索引
            to_keep = []
            # 记录已经处理过的标题
            processed_titles = []
            
            for idx, row in df.iterrows():
                current_title = str(row[title_column]).strip()
                
                # 检查是否与已处理的标题相似
                is_duplicate = False
                for processed_title in processed_titles:
                    similarity = difflib.SequenceMatcher(None, current_title, processed_title).ratio()
                    if similarity >= threshold:
                        is_duplicate = True
                        break
                
                if not is_duplicate:
                    to_keep.append(idx)
                    processed_titles.append(current_title)
            
            deduplicated_df = df.loc[to_keep].reset_index(drop=True)
            logger.info(f"【标题相似度去重完成】: 从 {len(df)} 条记录中去重后保留 {len(deduplicated_df)} 条")
            return deduplicated_df
        except Exception as e:
            logger.error(f"【标题相似度去重异常】: {str(e)}")
            return df


    def process_single_email(self, email_id):
        """处理单封邮件"""
        logger.info("=======读取邮件========")
        logger.info("4-1 邮件解析:")
        
        try:
            email_data, status_msg = process_email(self.mail, email_id)
            logger.info(f"\n{status_msg}")
            
            if not email_data:
                return
            
            # 打印邮件基本信息
            logger.info(f"邮件标题: {email_data['subject']}")
            logger.info(f"发件人: {email_data['from_email']}")
            logger.info(f"收件时间: {email_data['received_date']}")
            
            category = email_data['category']
            category_text = self.data_config.category_map.get(category, '未知')
            logger.info(f"识别为{category_text}数据")
            
            # 处理表格附件
            if not email_data['tables']:
                logger.warning("未找到表格附件")
                return
            
            logger.info(f"找到 {len(email_data['tables'])} 个表格附件")
            
            # 处理所有表格
            email_df = pd.DataFrame()
            for table in email_data['tables']:
                df = table['dataframe']
                
                # 将当前表格添加到邮件的DataFrame中
                if email_df.empty:
                    email_df = df.copy()
                else:
                    email_df = pd.concat([email_df, df], ignore_index=True)
            
            logger.info("4-3 表格格式转换完成")
            
            # 根据标题相似度去重
            if not email_df.empty:
                email_df = self.deduplicate_by_title_similarity(email_df)
            
            # 保存表格到本地
            if not email_df.empty:
                self.save_table(email_df, category, email_data, email_id)
            else:
                logger.warning("处理后的表格为空")
                
        except Exception as e:
            logger.error(f"【邮件处理异常】: {str(e)}")
    
    def save_table(self, df, category, email_data, email_id):
        """保存表格到本地"""
        try:
            # 设置信源类型
            source_type = self.data_config.source_type_map.get(category, '未知')
            df['信源类型'] = source_type
            
            # 标准化发布时间格式
            if '发布时间' in df.columns:
                df['发布时间'] = pd.to_datetime(df['发布时间'], format='%Y-%m-%d %H:%M:%S').astype(str)
            
            # 确保output目录存在
            output_dir = os.path.join(os.path.dirname(__file__), 'output')
            os.makedirs(output_dir, exist_ok=True)
            
            # 构建完整的文件路径
            category_text = self.data_config.category_map.get(category, '未知')
            output_file = os.path.join(output_dir, f"{email_data['time_slot']}{category_text}数据.xlsx")
            
            # 保存表格
            df.to_excel(output_file, index=False)
            logger.info("4-4 邮件表格保存到本地")
            
            
        except Exception as e:
            logger.error(f"【表格保存异常】: {str(e)}")
    

    def run(self):
        """执行邮件处理流程"""
        # 登录邮箱
        if not self.login():
            return
        
        # 选择收件箱
        if not self.select_inbox():
            return
        
        # 搜索邮件
        email_ids = self.search_emails()
        if not email_ids:
            logger.info("【无未读邮件】")
            return
        
        # 处理所有邮件
        logger.info("4. 处理邮件附件")
        for email_id in email_ids:
            self.process_single_email(email_id)
        
        logger.info("\n=====所有邮件处理完成======")
    
    def process_email(self, email_id):
        """处理邮件并返回邮件数据"""
        from .emailUtils import process_email as utils_process_email
        return utils_process_email(self.mail, email_id)


def main():
    """主函数"""
    # 创建配置实例
    email_config = EmailConfig()
    data_config = DataConfig()
    
    # 创建处理器并运行
    processor = EmailProcessor(email_config, data_config)
    processor.run()


if __name__ == '__main__':
    main()