import os, sys
from multinode.multinode_deployment import MultiNodeDeployment
from ray.runtime_env import RuntimeEnv
from fastapi import FastAPI, Response, Request
from dotenv import load_dotenv
load_dotenv() 


# 连接ray集群并获取集群状态  
# ray_address：
# None -- 已经启动集群则连接集群，未启动集群则在本机启动一个单机版ray服务
# "auto" -- 连接已经启动的集群，未启动集群报错
# "ray://<head-node-ip>:10001" -- 连接远程集群

# IMPORTANT! 远程启动需保持server环境和client环境的fastapi、starlette版本一致
headnode_ip = os.getenv("RAY_HEAD_IP")
multinode_depoly = MultiNodeDeployment(f"ray://{headnode_ip}:10001")
# multinode_depoly.shut_down(name="hello_world_fastapi")

# 定义任务类
app = FastAPI()
class MyFastAPIDeployment:  
    def __init__(self):
        pass
    
    @app.get("/") 
    async def root_func(self): 
        return Response("Hello, world!")
    
    @app.get("/test1") 
    async def test1(self):  
        return Response("Hello from test1!")
    
    @app.get("/test2") 
    async def test2(self): 
        return Response("Hello from test2!")


multinode_depoly.initialize_deployment( 
    name='hello_world_fastapi', 
    min_replicas=1,     # 最少副本数 
    max_replicas=2,     # 最多副本数 
    task_processor=MyFastAPIDeployment,  # 用户自定义任务API 
    app=app,  
    num_gpus=0,
) 
    
# 在集群环境启动服务 
multinode_depoly.run(port=8300)  


# 等待服务启动 
import time 
time.sleep(5) 

# 再次查看集群状态
multinode_depoly.check_cluster_status()

import requests
res = requests.get(multinode_depoly.url)
print(res.text)