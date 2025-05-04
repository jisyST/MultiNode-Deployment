import requests 
import asyncio 
import ray 
from ray import serve
from ray.serve import Deployment 
from ray.runtime_env import RuntimeEnv 
from fastapi import FastAPI 
 

class MultiNodeDeployment:   
    def __init__(self, ray_address: str = None): 
        """ 
        完成连接 Ray 并查看集群状态 
        ray_address:  
            None[缺省] -- 单机启动本地ray集群, 仅此方式可创建集群，其他均需先启动集群 
            "auto" -- 集群内自动发现并连接,自动发现并连接到同一网络内已存在的Ray集群,找不到抛出错误 
            "<head-node-ip>:<port>" - 集群内直接连接,需传入头节点ip和port 
            "ray://<head-node-ip>:10001" -- 集群外远程连接(Ray Client), 10001为头节点启动Ray Client服务默认监听端口
        """ 
        self.cluster_config = {}
        self.deployment = None 
        self.deployment_handle = None
        self.deployment_name = None 
        self.head_node_ip = None
        self.url = None
        self.check_cluster_status(ray_address, print_status=True)
        # if ray_address and ":" in ray_address:
        #     self.head_node_ip = ray_address.split(':')[-2].split('//')[-1]
     
    def check_cluster_status(self, ray_address: str = None, print_status: bool = False): 
        """ 
        启动 Ray 并查看集群状态 
 
        返回: 
            dict: 包含节点信息、总资源情况以及可用资源的字典。 
        """ 
        if not ray.is_initialized(): 
            if ray_address: 
                ray.init(address=ray_address) 
            else: 
                ray.init() 
                
        nodes = ray.nodes()
        resources = ray.cluster_resources()
        available_resources = ray.available_resources()
        
        self.cluster_config = { 
            "num_machines": len(nodes), 
            "num_gpus": resources.get('GPU', 0), 
            "num_cpus": resources.get('CPU', 0), 
            "available_gpus": available_resources.get('GPU', 0),
            "available_cpus": available_resources.get('CPU', 0), 
        }

        if not self.head_node_ip:
            for node in nodes:
                if node.get("IsHead", None) or node['Resources'].get("node:__internal_head__", None):
                    self.head_node_ip = node["NodeManagerAddress"].split(":")[0]
                    break
        if print_status:
            print("\n╔════════════════════════════╗") 
            print("║        🚀 集群状态         ║") 
            print("╠════════════════════════════╣") 
            print(f"║ 头节点IP: {self.head_node_ip:<16} ║") 
            print(f"║ 节点总数: {len(nodes):<16} ║") 
            print(f"║ CPU(总): {resources.get('CPU', 0):<17} ║") 
            print(f"║ CPU(可用): {available_resources.get('CPU', 0):<15} ║")
            print(f"║ GPU(总): {resources.get('GPU', 0):<17} ║") 
            print(f"║ GPU(可用): {available_resources.get('GPU', 0):<15} ║")
            print("╚════════════════════════════╝") 
         
        return self.cluster_config | {"aplications": serve.status().applications}
    
    def initialize_deployment(self, name: str, 
                              task_processor,
                              min_replicas: int = 1,
                            max_replicas: int = 1, 
                           num_gpus: int = 0, num_cpus: int = 1,
                           runtime_env: RuntimeEnv=None, 
                           app: FastAPI = None) -> Deployment: 
        """ 
        初始化部署任务对象 
 
        参数: 
            name (str): 部署名称，标识唯一的服务实例。 
            min_replicas (int): 最小副本数，控制服务的最低资源分配。 
            max_replicas (int): 最大副本数，决定服务在负载高时可扩展的最大副本数。 
            task_processor (Callable): 任务处理管道，封装推理或计算逻辑的可调用对象。 
            num_gpus (int, 可选): 每个副本所需的 GPU 数量，默认为 1。 
        """ 
        self.deployment_name = name 
        # if serve.get_deployment_handle(name):
        #     assert f"deployment {name} already exists!"
        if min_replicas * num_gpus > self.cluster_config.get('num_gpus', 0): 
            assert "Insufficient GPU resources!!" 
        if min_replicas * num_cpus > self.cluster_config.get('num_cpus', 0): 
            assert "Insufficient CPU resources!!" 
 
        # 定义 Ray Serve 部署类，内部封装 task_processor 的调用逻辑 
        ray_actor_options = {"num_gpus": num_gpus} 
        if runtime_env: 
            ray_actor_options["runtime_env"] = runtime_env 
        if not app:
            serve_deployment = serve.deployment( 
                name=name, 
                ray_actor_options=ray_actor_options, 
                autoscaling_config={ 
                    "min_replicas": min_replicas, 
                    "max_replicas": max_replicas, 
                }, 
            )(task_processor) 
             
        else: 
            serve_deployment = serve.deployment( 
                name=name, 
                ray_actor_options=ray_actor_options, 
                autoscaling_config={ 
                    "min_replicas": min_replicas, 
                    "max_replicas": max_replicas, 
                }, 
            )(serve.ingress(app)(task_processor)) 
        
        # 将部署绑定任务对象 
        self.deployment = serve_deployment.bind() 
    
    def connect_to_serve(self, name: str):
        self.deployment_handle = serve.get_deployment_handle(deployment_name=name, app_name=name)
        if not self.deployment_handle:
            assert f"deployment {name} not found!"
 
    def run(self, port: int = 8000, route_prefix: str = None): 
        """ 
        多节点启动已定义的部署任务 
 
        参数: 
            port (int): 服务监听的端口号。 
        """ 
        route_prefix = route_prefix or f"/{self.deployment_name}"
        if not route_prefix.startswith("/"):
            route_prefix = f"/{route_prefix}"
        
        if serve.context._global_client:
            current_port = serve.context._global_client._http_config.port
            if port != current_port:
                print(f"Serve is already running on port {current_port} !")
                port = current_port
                # raise RuntimeError(
                #     f"Serve is already running on port {current_port}. "
                #     "Please shut it down first to specify a different port."
                # )
            
        if not self.deployment: 
            raise ValueError("Empty deployment!") 
        serve.start(http_options={"port": port, "host": "0.0.0.0"}) 
        self.deployment_handle = serve.run(self.deployment, name=self.deployment_name, route_prefix=route_prefix) 
        self.url = f"http://{self.head_node_ip}:{port}{route_prefix}" 
        print(f"✅ 服务{self.deployment_name}已部署，访问地址：{self.url}")
     
    def inference(self, input_data: str = None): 
        """ 
        通过DeploymentHandle进行同步推理 
        """ 
        if not self.deployment_handle:
            assert "Serve not found!"
        try: 
            # 同步调用（适合单次请求） 
            result = self.deployment_handle.remote(input_data).result() 
            print("推理结果：", result) 
            return result 
        except Exception as e: 
            print(f"调用服务失败: {str(e)}") 
            return {"error": str(e)} 
 
    async def batch_forward(self, input_list): 
        """ 
        通过 DeploymentHandle 进行异步批量推理 
        """ 
        tasks = [self.deployment_handle.remote(data) for data in input_list]
        return await asyncio.gather(*tasks)
     
    def inference_url(self, input_data: str = None): 
        """ 
        通过 url 进行同步推理 
        """ 
        url = self.url 
        response = requests.post(url, json={"input": input_data}) 
        if response.status_code == 200: 
            print("推理结果：", response.json()) 
            return response.json() 
        else: 
            print("调用服务失败，状态码：", response.status_code) 
     
    async def batch_forward_url(self, input_list, max_retries=3): 
        """ 
        通过 url 进行异步批量推理 
        """ 
        url = self.url 
        results = [] 
 
        async def request_with_retry(data, retries): 
            for attempt in range(retries): 
                response = requests.post(url, json={"input": data}) # 需改为异步 
                if response.status_code == 200: 
                    return response.json() 
                await asyncio.sleep(1) 
            return {"error": "请求失败"} 
 
        tasks = [request_with_retry(data, max_retries) for data in input_list] 
        results = await asyncio.gather(*tasks) 
        return results 
 
    def shut_down(self, name: str = None): 
        if name:
            serve.delete(name)
        else:
            serve.shutdown()

 