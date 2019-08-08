# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


__all__ = [
    'create_component_from_airflow_op',
]


from typing import List
import json
from six import string_types
from ._python_op import _func_to_component_spec, _create_task_factory_from_component_spec


_default_airflow_base_image = 'apache/airflow:master-python3.6-ci' #TODO: Update a production release image once they become available: https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-10+Multi-layered+and+multi-stage+official+Airflow+CI+image#AIP-10Multi-layeredandmulti-stageofficialAirflowCIimage-ProposedsetupoftheDockerHubandTravisCI . See https://issues.apache.org/jira/browse/AIRFLOW-5093

def create_component_from_airflow_op(op_class, base_image=_default_airflow_base_image, result_output_name='Result', variables_dict_output_name='Variables', xcoms_dict_output_name='XComs', modules_to_capture: List[str] = None, use_code_pickling=True):
    if not isinstance(op_class, string_types):
        # Convert to string of class name
        op_class = str(op_class).split("'")[1].split('.')[-1].strip()

    def _build(*op_args, base_image=base_image, result_output_name=result_output_name, variables_dict_output_name=variables_dict_output_name, xcoms_dict_output_name=xcoms_dict_output_name, modules_to_capture=modules_to_capture, use_code_pickling=use_code_pickling, task_id=None, **op_kwargs):
        op = _create_component_from_airflow_op(op_class, *op_args, base_image=base_image, task_id=task_id, result_output_name=result_output_name, variables_dict_output_name=variables_dict_output_name, xcoms_dict_output_name=xcoms_dict_output_name, modules_to_capture=modules_to_capture, use_code_pickling=use_code_pickling, **op_kwargs)
        import json
        return op(json.dumps(op_args), json.dumps(op_kwargs))

    return _build

def _create_component_from_airflow_op(op_class, *op_args, base_image=_default_airflow_base_image, task_id=None, result_output_name='Result', variables_dict_output_name='Variables', xcoms_dict_output_name='XComs', modules_to_capture: List[str] = None, use_code_pickling=True, **op_kwargs):
    component_spec = _create_component_spec_from_airflow_op(op_class, *op_args, base_image=base_image, result_output_name=result_output_name, variables_dict_output_name=variables_dict_output_name, xcoms_dict_output_name=xcoms_dict_output_name, modules_to_capture=modules_to_capture, task_id=task_id, use_code_pickling=use_code_pickling, **op_kwargs)
    task_factory = _create_task_factory_from_component_spec(component_spec)
    return task_factory

def _create_component_spec_from_airflow_op(
    op_class,
    *op_args,
    base_image=_default_airflow_base_image,
    result_output_name='Result',
    variables_dict_output_name='Variables',
    xcoms_dict_output_name='XComs',
    variables_to_output=None,
    xcoms_to_output=None,
    modules_to_capture: List[str] = None,
    task_id=None,
    use_code_pickling=True,
    **op_kwargs
):
    variables_output_names = variables_to_output or []
    xcoms_output_names = xcoms_to_output or []
    modules_to_capture = modules_to_capture or []

    output_names = []
    if result_output_name is not None:
        output_names.append(result_output_name)
    if variables_dict_output_name is not None:
        output_names.append(variables_dict_output_name)
    if xcoms_dict_output_name is not None:
        output_names.append(xcoms_dict_output_name)
    output_names.extend(variables_output_names)
    output_names.extend(xcoms_output_names)
    from collections import namedtuple
    returnType = namedtuple('AirflowOpOutputs', output_names)

    def _run_airflow_op_closure(op_args, op_kwargs):
        import logging
        # Allow logs to show in Pipelines UI
        root = logging.getLogger()
        root.setLevel(logging.DEBUG)

        logging.info('Args: %s' % args)
        logging.info('Kwargs: %s' % kwargs)

        import json
        op_args = json.loads(op_args)
        op_kwargs = json.loads(op_kwargs)
        from airflow.utils import db
        db.initdb()

        from datetime import datetime
        from airflow import DAG, settings
        from airflow.models import TaskInstance, Variable, XCom

        execution_date = datetime.now()

        # Setup the DAG and Task
        import importlib
        Op = getattr(importlib.import_module("airflow.operators"), op_class)

        dag = DAG(dag_id='anydag', start_date=execution_date)

        op_kwargs['task_id'] = 'anytask'
        op_kwargs['dag'] = dag
        task = Op(*op_args, **op_kwargs)
        ti = TaskInstance(task=task, execution_date=execution_date)
        result = task.execute(ti.get_template_context())

        #variables = {var.id: var.val for var in settings.Session().query(Variable).all()}
        #xcoms = {msg.key: msg.value for msg in settings.Session().query(XCom).all()}
        #output_values = {}

        #import json
        #if result_output_name is not None:
        #    output_values[result_output_name] = str(result)
        #if variables_dict_output_name is not None:
        #    output_values[variables_dict_output_name] = json.dumps(variables)
        #if xcoms_dict_output_name is not None:
        #    output_values[xcoms_dict_output_name] = json.dumps(xcoms)
        #for name in variables_output_names:
        #    output_values[name] = variables[name]
        #for name in xcoms_output_names:
        #    output_values[name] = xcoms[name]

        #logging.info('Output: %s' % output_values)

        return result #'Complete' #returnType(**output_values)

    # Hacking the function signature so that correct component interface is generated
    import inspect
    #parameters = inspect.signature(op_class).parameters.values()
    #Filtering out `*args` and `**kwargs` parameters that some operators have
    #parameters = [param for param in parameters if param.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD]
    sig = inspect.Signature(
        parameters=inspect.signature(_run_airflow_op_closure).parameters.values(),
        return_annotation=returnType,
    )
    _run_airflow_op_closure.__signature__ = sig
    #_run_airflow_op_closure.__name__ = op_class.__name__

    return _func_to_component_spec(_run_airflow_op_closure, base_image=base_image, modules_to_capture=modules_to_capture, use_code_pickling=use_code_pickling, component_name=task_id)
