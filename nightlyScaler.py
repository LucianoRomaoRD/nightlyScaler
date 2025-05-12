from concurrent.futures import ThreadPoolExecutor, as_completed
from time import sleep
from kubernetes import client, config
from datadog_logger import DataDogLogger
import logging

logger = DataDogLogger(__name__)
logger.setLevel(logging.INFO)

def parse_quantity(cpu_str: str) -> int|float:
    if cpu_str.endswith('n'):
        return int(cpu_str[:-1]) / 1e6
    elif cpu_str.endswith('u'):
        return int(cpu_str[:-1]) / 1000
    elif cpu_str.endswith('m'):
        return int(cpu_str[:-1])
    else:
        return float(cpu_str) * 1000
    
def run_for_context(ctx):
    scaler = nightlyScaler(context=ctx)
    scaler.start_work()

class nightlyScaler:
    def __init__(self, context: str):
        self.context = context
        self.node_to_drain = []
        self.not_allowed_cluster = ['kafka', 'elastic', 'es-cm'] #Aqui podemos substituir por verificação de tags dos cluster no futuro.
        try:
            config.load_kube_config(context=context)
        except Exception as err:
            msg = f'Error - {err}'
            logger.error(msg)
            raise ValueError(msg)
        
        self.v1 = client.CoreV1Api()
    
    def get_node_capacity_usage(self) -> list:
        nodes = self.v1.list_node().items
        node_data = []

        if len(nodes) <= 2:
            logger.warning(f'Ignorando {self.context}, por ter apenas {len(nodes)} nodes')
            return node_data
        
        for node in nodes:
            name = node.metadata.name
            if "ondemand" not in name:
                continue

            alloc_cpu = parse_quantity(node.status.allocatable['cpu'])
            alloc_mem = node.status.allocatable['memory']
            alloc_mem_gi = int(alloc_mem[:-2]) / 1024 / 1024  # Ki -> Gi

            node_data.append({
                "name": name,
                "cpu_alloc": alloc_cpu,
                "mem_alloc_gi": alloc_mem_gi,
                "cpu_used": 0,
                "mem_used_gi": 0,
                "pod_count": 0
            })

        for node in node_data:
            node_name = node["name"]
            pods = self.v1.list_pod_for_all_namespaces(field_selector=f"spec.nodeName={node_name}").items

            for pod in pods:
                for container in pod.spec.containers:
                    cpu_req = container.resources.requests.get('cpu') if container.resources.requests else None
                    mem_req = container.resources.requests.get('memory') if container.resources.requests else None
                    cpu = parse_quantity(cpu_req) if cpu_req else 0

                    if mem_req:
                        if mem_req.endswith("Gi"):
                            mem = float(mem_req.replace("Gi", ""))
                        elif mem_req.endswith("Mi"):
                            mem = float(mem_req.replace("Mi", "")) / 1024
                        elif mem_req.endswith("Ki"):
                            mem = float(mem_req.replace("Ki", "")) / 1024 / 1024
                        else:
                            mem = 0
                    else:
                        mem = 0

                    node["cpu_used"] += cpu
                    node["mem_used_gi"] += mem
                    node["pod_count"] += 1
        return node_data

    def check_drain(self, node_name: str, cpu_usada: int|float, cpu_total: int|float, mem_usada_gi: int|float, pod_count: int, media_pods_cluster: int):
        cpu_percent = cpu_usada / cpu_total if cpu_total else 0
        mem_percent = mem_usada_gi / 120
        pod_percent = pod_count / media_pods_cluster if media_pods_cluster else 1

        if cpu_percent < 0.55 and mem_percent < 0.75 and pod_percent < 0.75:
            logger.info(f"{self.context} {node_name} ✅ DRENAR – Subutilizado")
            self.node_to_drain.append(node_name)
        elif cpu_percent < 0.65 and pod_percent < 0.85:
            logger.info(f"{self.context} {node_name} ⚠️  POTENCIAL DRENO")
            self.node_to_drain.append(node_name)
        elif cpu_percent > 0.9 or mem_percent > 0.9:
            logger.info(f"{self.context} {node_name} 🔥 HOT NODE")   
    
    def drain(self) -> None:
        for node in self.node_to_drain:
            logger.warning(f" TESTE NÃO É UMA AÇÃO REAL - Realizando Drain do node {node} no cluster {self.context}")
            print(f"Realizando Drain do node {node} no cluster {self.context}")
          
    def start_work(self) -> None:
        if any(item in self.context.lower() for item in self.not_allowed_cluster):
            logger.info(f"Skippando {self.context} por segurança, lista de negação de cluster ativa!")
            return None
        
        try:
            self.v1.list_node(_request_timeout=10)
        except Exception as e:
                logger.error(f'Timeout ao realizar verificação no cluster {self.context} - Skipping')
                return None

        nodes_drain = self.get_node_capacity_usage()

        if not nodes_drain:
            return None
        
        pod_counts = [n["pod_count"] for n in nodes_drain if n["pod_count"] > 0]
        media_pods = sum(pod_counts) / len(pod_counts) if pod_counts else 1
        for node in nodes_drain:
            self.check_drain(
                node['name'],
                node['cpu_used'],
                node['cpu_alloc'],
                node['mem_used_gi'],
                node['pod_count'],
                media_pods
            )
        self.drain()
        

if __name__ == "__main__":

    contexts,_ = config.list_kube_config_contexts()
    ctx_names = [c["name"] for c in contexts if "eks" not in c["name"]]
    
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = { pool.submit(run_for_context, ctx): ctx for ctx in ctx_names }

        for future in futures:
            try:
                future.result(timeout=60)
            except TimeoutError:
                ctx = futures[future]
                logger.error(f"[{ctx}] Timeout após 10s")
            except Exception as e:
                ctx = futures[future]
                logger.error(f"[{ctx}] Erro: {e}")
    
    logger.stop()
    logging.shutdown()