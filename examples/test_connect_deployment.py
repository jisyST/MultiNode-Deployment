
import os, sys
import asyncio
from multinode.multinode_deployment import MultiNodeDeployment
from dotenv import load_dotenv
load_dotenv() 

# 连接服务并调用
headnode_ip = os.getenv("RAY_HEAD_IP")
multinode_depoly = MultiNodeDeployment(f"ray://{headnode_ip}:10001")

multinode_depoly.connect_to_serve("hello_world")
multinode_depoly.inference("hello123")

input_data = ['BATCH TASK' + str(i) for i in range(50)] 
print(asyncio.run(multinode_depoly.batch_forward(input_data)))