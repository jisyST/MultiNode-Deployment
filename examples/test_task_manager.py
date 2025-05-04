import os
from multinode.task_manager import TaskManager
from multinode.multinode_deployment import MultiNodeDeployment
from dotenv import load_dotenv
load_dotenv() 

headnode_ip = os.getenv("RAY_HEAD_IP")
multinode_depoly = MultiNodeDeployment(f"ray://{headnode_ip}:10001")

multinode_depoly.connect_to_serve("hello_world")

# TODO 异常怎么传？
# 初始化任务管理器
manager = TaskManager(task_name="hello_world", engine=multinode_depoly, batch_size=3)

# 加载任务数据
tasks = [f"input_{i}" for i in range(10)]
manager.load_tasks(tasks)

# 启动任务执行（可多次调用，支持恢复未完成任务）
manager.run_tasks()