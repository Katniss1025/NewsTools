import pandas as pd
import os
import sys
import logging

# 添加当前目录到系统路径，以便导入email_tools
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from email_tools.emailUtils import connect_mail, mark_email_as_read, select_mail, search_mail, process_email, send_email_with_attachment

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
    
    def transform_table_columns(self, df, category):
        """
        根据数据类别转换表格列格式
        
        参数:
            df: 原始DataFrame
            category: 数据类别 (0: 境内数据, 1: 境外数据)
        
        返回:
            转换后的DataFrame
        """
        try:
            # 创建副本避免修改原数据
            new_df = df.copy()
            
            # 根据类别选择目标列
            target_columns = self.data_config.gw_target_columns if category == 1 else self.data_config.gn_target_columns
            
            # 处理境内数据的特殊映射：公众号名称对应网站名称
            if category == 0 and '作者名' in new_df.columns:
                new_df = new_df.rename(columns={'作者名': '公众号名称'})
            
            # 添加缺失字段（保持原始数据不变）
            for col in target_columns:
                if col not in new_df.columns:
                    new_df[col] = ''
            
            # 按目标顺序重新排列列
            return new_df[target_columns]
        except Exception as e:
            logger.error(f"【表格转换异常】: {str(e)}")
            return None
    
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
                
                # 转换为目标格式
                transformed_df = self.transform_table_columns(df, category)
                if transformed_df is None:
                    continue
                
                # 将当前表格添加到邮件的DataFrame中
                if email_df.empty:
                    email_df = transformed_df.copy()
                else:
                    email_df = pd.concat([email_df, transformed_df], ignore_index=True)
            
            logger.info("4-3 表格格式转换完成")
            
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
            
            # 发送邮件和标记已读（已注释，不执行）
            self.send_email_and_mark_read(output_file, category_text, email_data, email_id)
            
        except Exception as e:
            logger.error(f"【表格保存异常】: {str(e)}")
    
    def send_email_and_mark_read(self, output_file, category_text, email_data, email_id):
        """发送邮件并标记已读"""
        try:
            # 发送邮件
            logger.info("4-5 发送日报邮件：")
            email_subject = f"{category_text}数据报表 - {email_data['time_slot']}"
            email_body = f"您好，\n\n这是时间段 {email_data['time_slot']} 的{category_text}数据报表，请查收附件。\n\n此致\n系统自动发送"
            
            send_status = send_email_with_attachment(
                self.email_config.smtp_server, 
                self.email_config.smtp_port, 
                self.email_config.address, 
                self.email_config.password,
                self.email_config.to_email, 
                email_subject, 
                email_body, 
                output_file
            )
            
            # 将邮件标记为已读
            if send_status:
                logger.info("【邮件发送成功，标记为已读】")
                # mark_email_as_read(self.mail, email_id)
            else:
                logger.error("【邮件发送失败】")
                
        except Exception as e:
            logger.error(f"【发送邮件异常】: {str(e)}")
    
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
        from emailUtils import process_email as utils_process_email
        return utils_process_email(self.mail, email_id)
    
    def mark_email_as_read(self, email_id):
        """标记邮件为已读"""
        from emailUtils import mark_email_as_read as utils_mark_email_as_read
        try:
            utils_mark_email_as_read(self.mail, email_id)
            logger.info(f"邮件 {email_id} 标记为已读成功")
            return True
        except Exception as e:
            logger.error(f"标记邮件为已读失败: {str(e)}")
            return False


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