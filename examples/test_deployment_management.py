import os
from multinode.multinode_deployment import MultiNodeDeployment
from dotenv import load_dotenv
load_dotenv() 

# 连接服务并查看状态
headnode_ip = os.getenv("RAY_HEAD_IP")
multinode_depoly = MultiNodeDeployment(f"ray://{headnode_ip}:10001")
status = multinode_depoly.check_cluster_status()
for k, v in status.items():
    print(f"{k} : {v}")

# 关掉hello world并查看状态
multinode_depoly.shut_down("hello_world")
status = multinode_depoly.check_cluster_status()
for k, v in status.items():
    print(f"{k} : {v} \n")
