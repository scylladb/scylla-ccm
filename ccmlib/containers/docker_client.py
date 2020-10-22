import logging
import socket
from random import randrange
import docker
import docker.errors

LOGGER = logging.getLogger(__name__)


class BaseDocker:
    def __init__(self, name, image, port, shell_type='bash', is_clear_container=True, volumes=None,  **create_kwargs):
        self.name = str(name)
        self.image = image
        self.shell_type = shell_type
        self.container = None
        self.container_pid = None
        self.container_name = None
        self.client = docker.from_env()
        self.volumes = volumes

        self.pull()
        self.container_pid, self.container_name = self.create_container(
            ports={"9180/tcp": port}, volumes=volumes, is_clear_container=is_clear_container, **create_kwargs)

    def pull(self):
        LOGGER.debug("Checking if the '{}' docker image is exists".format(self.image))
        try:
            if not self.client.images.list(self.image):
                LOGGER.debug("The '{}' docker image not found.\nTrying to pulls it from registry".format(self.image))
                self.client.images.pull(self.image)
        except docker.errors.APIError as err:
            LOGGER.error("Could not pull the '{}' docker image".format(self.image), exc_info=err)
            raise err

    def remove_old_containers(self, is_remove_current_container=True):
        LOGGER.debug("Searching for all running containers")
        running_containers = self.client.containers.list(all=True)
        if running_containers:
            LOGGER.debug("Remove the following running containers:{}".format("\n".join(running_containers)))
            for container in running_containers:
                if is_remove_current_container and self.container.name == container.name:
                    continue
                container.remove(force=True)

    def create_container(self, ports=None, volumes=None, is_clear_container=True, **kwargs):
        if is_clear_container:
            self.remove_old_containers()
        LOGGER.debug("Creating container using image '{}'".format(self.image))
        self.container = self.client.containers.create(
            image=self.image, hostname=self.name, detach=True, stdin_open=True, tty=True, ports=ports, volumes=volumes,
            name=self.name, network_disabled=False, privileged=True, pid_mode="host", **kwargs)
        LOGGER.debug("Start container '{}'".format(self.name))
        self.container.start()
        return self.container.attrs["State"]["Pid"], self.container.name

    def close(self):
        if not self.container or self.is_running():
            raise docker.errors.DockerException("The '{}' docker was closed!".format(self.container.name))

        container_name = self.container.name
        LOGGER.debug(
            "Stop the '{}' container (The current state is '{}') is".format(container_name, self.container.status))
        self.container.stop(timeout=1)
        LOGGER.debug("Remove the '{}' container".format(container_name))
        try:
            self.container.remove(force=True)
        except docker.errors.APIError as err:
            LOGGER.warning("Failed to remove '{}' container".format(self.image), exc_info=err)
        self.container = self.container_name = self.container_pid = None
        LOGGER.info("The'{}' container is closed".format(self.name))

    def execute_command(self, command, is_stream=True, **kwargs):
        if self.container is None:
            raise docker.errors.DockerException("container is not running".format(self.name, command))

        command = f"{self.shell_type} -c \"{command}\""
        LOGGER.debug("Execute '{}' command in '{}' container".format(command, self.name))
        try:
            output = self.container.exec_run(command, stream=is_stream, **kwargs)[1]
        except docker.errors.APIError as err:
            if not self.is_running():
                self.close()
            raise docker.errors.DockerException("Docker container API error: {}".format(err))
        if is_stream:
            response = "".join([line.decode("utf-8", "ignore").rstrip() for line in output])
            LOGGER.debug("The response of '{}' command is:\n{}".format(command, response))
            output = response

        return output

    def is_running(self):
        """
        Check if class container state is running
        :return: boolean
        """
        try:
            return self.container.status == "running"
        except docker.errors.APIError:
            return False

    @staticmethod
    def server_ip(destination_address="8.8.8.8"):
        socket_connection = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            for _ in range(5):
                socket_connection.connect((destination_address, randrange(50000, 60000)))
                break
        except OSError:
            pass
        finally:
            ip = socket_connection.getsockname()[0]
        return ip

    def stop_service(self, service_name):
        """
        This method stops the given service name
        :param service_name: str name of the service
        """
        LOGGER.info(f"Stopping the service {service_name} in container {self.name}")
        self.execute_command(f"service {service_name} stop")

    def start_service(self, service_name):
        """
        This method starts the given service name
        :param service_name: str name of the service
        """
        LOGGER.info(f"Starting the service {service_name} in container {self.name}")
        self.execute_command(f"service {service_name} start")

    def restart_service(self, service_name):
        """
        This method restarts the given service name
        :param service_name: str name of the service
        """
        LOGGER.info(f"Restarting the service {service_name} in container {self.name}")
        self.execute_command(f"service {service_name} restart")

    @property
    def logs(self):
        if not self.container:
            raise docker.errors.DockerException("The '{}' container is not running".format(self.name))

        return self.container.logs().decode("utf-8", "ignore").splitlines()
