import os, sys
from multinode.multinode_deployment import MultiNodeDeployment
import torch
from fastapi import Request 
import requests
from dotenv import load_dotenv
load_dotenv() 

############################
# 张量并行
############################

# 连接ray集群并获取集群状态  
# ray_address：
# None -- 已经启动集群则连接集群，未启动集群则在本机启动一个单机版ray服务
# "auto" -- 连接已经启动的集群，未启动集群报错
# "ray://<head-node-ip>:10001" -- 连接远程集群
headnode_ip = os.getenv("RAY_HEAD_IP")
multinode_depoly = MultiNodeDeployment(f"ray://{headnode_ip}:10001")

# 定义任务类 
class MyDeployment:      
    def __init__(self):
        # 初始化时设置模型并行，使用2张GPU（tensor_parallel_size=2）。
        # 请确保在 Ray Serve 部署时，ray_actor_options 中配置了 num_gpus=2，
        # 这样 Ray 会在分配该进程时设置 CUDA_VISIBLE_DEVICES 环境变量，只显示2张GPU。
        from vllm import LLM, SamplingParams
        cuda_visible = os.environ.get("CUDA_VISIBLE_DEVICES", "Not Set")
        print(f"[Init] CUDA_VISIBLE_DEVICES: {cuda_visible}")

        # 打印当前 GPU 情况
        print(f"[Init] torch.cuda.device_count: {torch.cuda.device_count()}")
        for i in range(torch.cuda.device_count()):
            print(f"[Init] GPU {i}: {torch.cuda.get_device_name(i)}")
            
        self.llm = LLM("/home/mnt/share_server/models/Qwen2-7B-Instruct", tensor_parallel_size=2)
        self.sampling_params = SamplingParams(temperature=0.7, top_p=0.9, max_tokens=512)

    async def __call__(self, request: Request):
        body = await request.json()
        prompt = body.get("prompt", "")

        outputs = self.llm.generate(prompt, self.sampling_params)
        return {"output": outputs[0].outputs[0].text}


# 初始化部署对象   
multinode_depoly.initialize_deployment( 
    name='hello_world', 
    min_replicas=1,     # 最少副本数 
    max_replicas=1,     # 最多副本数 
    task_processor=MyDeployment,  # 用户自定义任务API 
    num_gpus=2,
) 
    
# 在集群环境启动部署对象
multinode_depoly.run(port=8100) 


# 等待服务启动 
import time 
time.sleep(5) 


# 再次查看集群状态
multinode_depoly.check_cluster_status()


# 单次推理 
API_URL = multinode_depoly.url

# 构造请求体
data = {
    "prompt": "请简要介绍一下量子计算的基本原理。"
}

# 发起 POST 请求
response = requests.post(API_URL, json=data)

# 打印返回结果
if response.ok:
    print("🧠 模型回答：", response.json()["output"])
else:
    print("❌ 请求失败：", response.status_code, response.text)

# 关闭服务
multinode_depoly.shut_down() 