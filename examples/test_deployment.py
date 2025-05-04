import os, sys
from multinode.multinode_deployment import MultiNodeDeployment
from ray.runtime_env import RuntimeEnv
import torch
from ray import get_runtime_context 
import asyncio
from fastapi import Request 
from dotenv import load_dotenv
load_dotenv()  





# 连接ray集群并获取集群状态  
# ray_address：
# None -- 已经启动集群则连接集群，未启动集群则在本机启动一个单机版ray服务
# "auto" -- 连接已经启动的集群，未启动集群报错
# "ray://<head-node-ip>:10001" -- 连接远程集群
headnode_ip = os.getenv("RAY_HEAD_IP")
multinode_depoly = MultiNodeDeployment(f"ray://{headnode_ip}:10001")
multinode_depoly.shut_down()

# 定义任务类 
class MyDeployment:     
    async def __call__(self, input, *args, **kargs): 
        if isinstance(input, Request):
            input = "get request"
        context = get_runtime_context() 
        node_ip = context.get_node_id()  # 获取当前节点 ID（Ray 分配的唯一标识） 
            
        # 获取当前使用的 GPU ID（假设使用 PyTorch） 
        if torch.cuda.is_available(): 
            gpu_id = torch.cuda.current_device() 
            gpu_name = torch.cuda.get_device_name(gpu_id) 
        else: 
            gpu_id = "CPU" 
            gpu_name = "No GPU available" 

        # 打印日志 
        print(f"🚀🚀🚀 推理任务{input}运行在：Node {node_ip}，GPU {gpu_id} ({gpu_name})") 
        return f"node {node_ip} got {input}" 

# 初始化部署对象   
multinode_depoly.initialize_deployment( 
    name='hello_world', 
    min_replicas=2,     # 最少副本数 
    max_replicas=2,     # 最多副本数 
    task_processor=MyDeployment,  # 用户自定义任务API 
    num_gpus=1,
) 
    
# 在集群环境启动部署对象
multinode_depoly.run(port=8100) 


# 等待服务启动 
import time 
time.sleep(5) 


# 再次查看集群状态
multinode_depoly.check_cluster_status()


# 单次推理 
print(multinode_depoly.inference("SINGLE TASK")) 
time.sleep(3)
    
# 批量推理 
input_data = ['BATCH TASK' + str(i) for i in range(50)] 
asyncio.run(multinode_depoly.batch_forward(input_data))

# 关闭服务
# multinode_depoly.shut_down() 