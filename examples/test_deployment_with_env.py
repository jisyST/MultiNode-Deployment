import os, sys
from multinode.multinode_deployment import MultiNodeDeployment
from ray.runtime_env import RuntimeEnv
from dotenv import load_dotenv
load_dotenv()


headnode_ip = os.getenv("RAY_HEAD_IP")
multinode_depoly = MultiNodeDeployment(f"ray://{headnode_ip}:10001")
multinode_depoly.shut_down()

# 定义任务类 
class MyDeployment:      
    async def __call__(self, input, *args, **kargs): 
        import lxml_html_clean
        return "success"

# 需安装virtualenv包来管理虚拟环境
runtime_env = RuntimeEnv(
    pip={"packages":["lxml-html-clean"], "pip_check": False,
    "pip_version": "==22.3.1;python_version=='3.10.9'"})

# 初始化部署对象   
multinode_depoly.initialize_deployment( 
    name='hello_world', 
    min_replicas=1,     # 最少副本数 
    max_replicas=1,     # 最多副本数 
    task_processor=MyDeployment,  # 用户自定义任务API 
    num_gpus=0,
    runtime_env=runtime_env
) 
    
# 在集群环境启动部署对象
multinode_depoly.run(port=8100) 


# 等待服务启动 
import time 
time.sleep(5) 


# 再次查看集群状态
multinode_depoly.check_cluster_status()


# 单次推理 
print(multinode_depoly.inference("[SINGLE TASK]")) 

# 关闭服务
multinode_depoly.shut_down() 