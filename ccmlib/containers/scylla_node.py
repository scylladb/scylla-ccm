from ccmlib.containers.docker_client import BaseDocker


class ScyllaNode(BaseDocker):
    def __init__(self, name, image, port, **create_kwargs):
        super().__init__(name, image, port, **create_kwargs)


if __name__ == '__main__':
    ScyllaNode(name="ScyllaNodeDocker1", image="scylladb/scylla:4.1.8", port=20000)
