import os
import json
import uuid
from datetime import datetime
import asyncio
from typing import List, Optional, Callable
from sqlalchemy import create_engine, Column, Integer, String, DateTime, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
# from multinode_deployment import MultiNodeDeployment

Base = declarative_base()
DB_URL = "sqlite:///data/test.db"

# 任务状态定义类
class TaskStatus:
    PENDING = 'pending'         # 待处理
    PROCESSING = 'processing'   # 处理中
    COMPLETED = 'completed'     # 已完成
    FAILED = 'failed'           # 失败

# 任务数据模型
# class TaskModel(Base):
#     __tablename__ = 'tasks'
#     task_id = Column(String, primary_key=True)        # 任务唯一 ID
#     input_data = Column(JSON)                       # 输入数据
#     status = Column(String, default=TaskStatus.PENDING)  # 当前状态，默认为待处理
#     retries = Column(Integer, default=0)              # 重试次数
#     result = Column(JSON)                           # 推理结果
#     created_at = Column(DateTime, default=datetime.now)   # 创建时间
#     updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)  # 任务 完成/失败 时间

def create_task_model(table_name: str):
    """根据指定表名动态创建 TaskModel 类"""
    class TaskModel(Base):
        __tablename__ = table_name
        task_id = Column(String, primary_key=True)
        input_data = Column(JSON)
        status = Column(String, default=TaskStatus.PENDING)
        retries = Column(Integer, default=0)
        result = Column(JSON)
        created_at = Column(DateTime, default=datetime.now)
        updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    return TaskModel

# 任务管理器
class TaskManager:
    """
    TaskManager 是一个批量任务调度与推理执行管理类。

    功能包括：
    - 将任务加载入数据库并设为待处理状态；
    - 按批次调度任务并通过推理引擎执行；
    - 支持结果保存至数据库或自定义处理函数；
    - 自动处理失败重试、状态更新；
    - 提供任务进度监控和结果查询接口。

    参数：
    - db_url (str): 数据库连接字符串，用于任务持久化；
    - engine: 推理引擎对象，需实现 batch_forward(inputs: List[str]) -> List[Any]；
    - process_result_func (Optional[Callable]): 可选的自定义结果处理函数；
    - batch_size (int): 每批推理处理的任务数量，默认值为 16。
    """

    def __init__(self, task_name: str, engine, process_result_func: Optional[Callable] = None, batch_size: int = 16):
        self.engine = engine                                # 推理引擎，需实现 batch_forward 方法
        self._process_result = process_result_func          # 自定义结果处理函数，如不传入，则将结果写入数据库result字段
        self.batch_size = batch_size                        # 每批任务处理数量

        self._engine = create_engine(DB_URL)                # 创建数据库引擎
        self.task_model = create_task_model(task_name)
        Base.metadata.create_all(self._engine)             # 创建表结构
        self.Session = sessionmaker(bind=self._engine)     

    # 加载任务数据到数据库
    def load_tasks(self, tasks: List[str], clear_existing_data: bool = True):
        session = self.Session()
        try:
            if clear_existing_data:
                session.query(self.task_model).delete()
                session.commit()
            for input_data in tasks:
                task = self.task_model(
                    task_id=str(uuid.uuid4()),
                    input_data=input_data,
                    status=TaskStatus.PENDING
                )
                session.add(task)
            session.commit()
        finally:
            session.close()

    # 执行任务调度和推理
    def run_tasks(self):
        session = self.Session()
        try:
            # 将可能因中断而卡住的处理中任务恢复为待处理
            session.query(self.task_model).filter(self.task_model.status == TaskStatus.PROCESSING).update({self.task_model.status: TaskStatus.PENDING})
            session.commit()
            while True:
                # 查询待处理任务
                tasks = session.query(self.task_model).filter(self.task_model.status == TaskStatus.PENDING).limit(self.batch_size).all()
                if not tasks:
                    break
                
                inputs = []
                task_map = {}
                for task in tasks:
                    task.status = TaskStatus.PROCESSING
                    task_map[task.task_id] = task
                    inputs.append(task.input_data)
                session.commit()
                try:
                    if asyncio.iscoroutinefunction(self.engine.batch_forward):
                        results = asyncio.run(self.engine.batch_forward(inputs))
                    else:
                        results = self.engine.batch_forward(inputs)
                except Exception as batch_e:
                    # 批量处理失败，标记每个任务状态为待处理或失败
                    for task in tasks:
                        task.retries += 1
                        if task.retries > 3:
                            task.status = TaskStatus.FAILED
                        else:
                            task.status = TaskStatus.PENDING
                    session.commit()
                    continue 

                for task, result in zip(tasks, results):
                    if result is None:  # 判定失败条件，待修改
                        task.retries += 1
                        if task.retries >= 3:
                            task.status = TaskStatus.FAILED
                        else:
                            task.status = TaskStatus.PENDING
                    else:
                        if self._process_result:
                            self._process_result(task.task_id, result)  # 使用自定义处理函数
                            task.result = None
                        else:
                            task.result = json.dumps(result)            # 默认保存结果为 JSON 字符串
                        task.status = TaskStatus.COMPLETED
                    task.updated_at = datetime.now()
                session.commit()
                self._print_task_status(session)                 # 实时打印状态
        finally:
            session.close()

    # 获取任务整体状态
    def _get_task_status(self):
        session = self.Session()
        try:
            total = session.query(self.task_model).count()                         # 总任务数
            completed = session.query(self.task_model).filter(self.task_model.status == TaskStatus.COMPLETED).count()
            failed = session.query(self.task_model).filter(self.task_model.status == TaskStatus.FAILED).count()
            pending = session.query(self.task_model).filter(self.task_model.status == TaskStatus.PENDING).count()
            processing = session.query(self.task_model).filter(self.task_model.status == TaskStatus.PROCESSING).count()
            percent = completed / total * 100 if total else 0
            return {
                "total": total,
                "completed": completed,
                "failed": failed,
                "pending": pending,
                "processing": processing,
                "percent": f"{percent:.2f}%"
            }
        finally:
            session.close()

    # 打印当前任务进度
    def _print_task_status(self, session):
        total = session.query(self.task_model).count()
        completed = session.query(self.task_model).filter(self.task_model.status == TaskStatus.COMPLETED).count()
        failed = session.query(self.task_model).filter(self.task_model.status == TaskStatus.FAILED).count()
        print(f"任务进度: 已完成 {completed}/{total}, 失败 {failed}")
    
    # 查询单个任务的推理结果
    def get_result(self, task_id: str) -> Optional[str]:
        session = self.Session()
        try:
            task = session.query(self.task_model).filter(self.task_model.task_id == task_id).first()
            if task:
                return task.result
            return None
        finally:
            session.close()

    # 返回所有结果
    def iter_results(self):
        results = []
        session = self.Session()
        try:
            for task in session.query(self.task_model).filter(self.task_model.status == TaskStatus.COMPLETED):
                # print(f"Input: {task.input_data}\nResult: {task.result}\n")
                results.append({"input": task.input_data, "output": task.result})
        finally:
            session.close()
        return results


if __name__ == "__main__":
    import time
    class DummyEngine:
        def batch_forward(self, inputs):
            time.sleep(1)  # 模拟耗时
            print([f"{inp}_pred" if inp != "input_2" else None for inp in inputs])
            return [f"{inp}_pred" if inp != "input_2" else None for inp in inputs]


    # TODO 异常怎么传？
    # 初始化任务管理器
    manager = TaskManager(task_name="hellooo", engine=DummyEngine(), batch_size=3)

    # 加载任务数据（仅第一次加载）
    tasks = [f"input_{i}" for i in range(10)]
    manager.load_tasks(tasks)

    # 启动任务执行（可多次调用，支持恢复未完成任务）
    manager.run_tasks()

    # 打印结果
    manager.iter_results()
    
    

