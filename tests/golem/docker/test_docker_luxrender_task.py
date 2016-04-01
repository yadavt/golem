import jsonpickle
import logging
import shutil
from os import makedirs, path

import gnr.node
from golem.core.common import get_golem_path
from golem.task.taskbase import result_types
from golem.task.taskcomputer import DockerTaskThread
from golem.task.taskserver import TaskServer
from gnr.task.luxrendertask import LuxRenderTaskBuilder
from golem.tools.testwithappconfig import TestWithAppConfig

from test_docker_image import DockerTestCase

# Make peewee logging less verbose
logging.getLogger("peewee").setLevel("INFO")


# TODO: extract code common to this class and TestDockerBlenderTask to a superclass
# TODO: test luxrender tasks with .flm file
class TestDockerLuxrenderTask(TestWithAppConfig, DockerTestCase):

    TASK_FILE = "docker-luxrender-test-task.json"

    def setUp(self):
        TestWithAppConfig.setUp(self)
        self.error_msg = None
        self.dirs_to_remove = []
        self.task_computer_send_task_failed = TaskServer.send_task_failed

    def tearDown(self):
        for dir in self.dirs_to_remove:
            shutil.rmtree(dir)
        TaskServer.send_task_failed = self.task_computer_send_task_failed
        TestWithAppConfig.tearDown(self)

    def _test_task_definition(self):
        task_file = path.join(path.dirname(__file__), self.TASK_FILE)
        with open(task_file, "r") as f:
            task_def = jsonpickle.decode(f.read())

        # Replace $GOLEM_DIR in paths in task definition by get_golem_path()
        golem_dir = get_golem_path()

        def set_root_dir(p): return p.replace("$GOLEM_DIR", golem_dir)

        task_def.resources = set(set_root_dir(p) for p in task_def.resources)
        task_def.main_scene_file = set_root_dir(task_def.main_scene_file)
        task_def.main_program_file = set_root_dir(task_def.main_program_file)
        task_def.output_file = set_root_dir(task_def.output_file)
        return task_def

    def _test_task(self):
        task_def = self._test_task_definition()
        node_name = "0123456789abcdef"
        root_path = get_golem_path()
        task_builder = LuxRenderTaskBuilder(node_name, task_def, root_path)
        render_task = task_builder.build()
        render_task.__class__._update_task_preview = lambda self_: ()
        return render_task

    def _run_docker_task(self, render_task, timeout=0):
        task_id = render_task.header.task_id
        ctd = render_task.query_extra_data(1.0)

        # Create the computing node
        node = gnr.node.GNRNode()
        node.initialize()

        task_computer = node.client.task_server.task_computer
        resource_dir = task_computer.resource_manager.get_resource_dir(task_id)
        temp_dir = task_computer.resource_manager.get_temporary_dir(task_id)
#        self.dirs_to_remove.append(resource_dir)
#        self.dirs_to_remove.append(temp_dir)

        # Copy the task resources
        common_prefix = path.commonprefix(render_task.task_resources)
        common_prefix = path.dirname(common_prefix)

        for res_file in render_task.task_resources:
            dest_file = path.join(resource_dir,
                                  res_file[len(common_prefix) + 1:])
            dest_dirname = path.dirname(dest_file)
            if not path.exists(dest_dirname):
                makedirs(dest_dirname)
            shutil.copyfile(res_file, dest_file)

        def send_task_failed(self_, subtask_id, task_id, error_msg, *args):
            self.error_msg = error_msg

        TaskServer.send_task_failed = send_task_failed

        # Start task computation
        task_computer.task_given(ctd, timeout)
        result = task_computer.resource_given(ctd.task_id)
        self.assertTrue(result)

        # Thread for task computation should be created by now
        task_thread = None
        with task_computer.lock:
            if task_computer.current_computations:
                task_thread = task_computer.current_computations[0]

        if task_thread:
            task_thread.join(60.0)

        return task_thread, self.error_msg, temp_dir

    def test_luxrender_subtask(self):
        task = self._test_task()
        task_thread, error_msg, out_dir = self._run_docker_task(task)
        self.assertIsInstance(task_thread, DockerTaskThread)
        self.assertIsNone(error_msg)

        # Check the number and type of result files:
        result = task_thread.result
        self.assertEqual(result["result_type"], result_types["files"])
        self.assertGreaterEqual(len(result["data"]), 3)
        self.assertTrue(
            any([path.basename(f) == DockerTaskThread.STDOUT_FILE for f in result["data"]]))
        self.assertTrue(
            any([path.basename(f) == DockerTaskThread.STDERR_FILE for f in result["data"]]))
        self.assertTrue(
            any([f.endswith(".flm") for f in result["data"]]))


