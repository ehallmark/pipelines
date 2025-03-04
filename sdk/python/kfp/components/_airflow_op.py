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

from ._python_op import _func_to_component_spec, _create_task_factory_from_component_spec
_default_airflow_base_image = 'apache/airflow@sha256:7f60cbef6bf92b1f3a5b4e46044911ced39736a8c3858284d3c5a961b3ba8735'


def create_component_from_airflow_op(base_image=_default_airflow_base_image, result_output_name='Result', variable_output_names=None, xcom_output_names=None, modules_to_capture: List[str] = None, task_id=None):
    component_spec = _create_component_spec_from_airflow_op(base_image, result_output_name, variable_output_names, xcom_output_names, modules_to_capture, task_id=task_id)
    task_factory = _create_task_factory_from_component_spec(component_spec)
    return task_factory


def _create_component_spec_from_airflow_op(
    base_image,
    result_output_name='Result',
    variables_dict_output_name='Variables',
    xcoms_dict_output_name='XComs',
    variables_to_output=None,
    xcoms_to_output=None,
    modules_to_capture: List[str] = None,
    task_id=None,
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

    def _run_airflow_op_closure():
        def _airflow(op_class, kwargs_str=''):
            result_output_name='Result'
            variables_dict_output_name='Variables'
            xcoms_dict_output_name='XComs'
            variables_to_output=None
            xcoms_to_output=None
            variables_output_names = variables_to_output or []
            xcoms_output_names = xcoms_to_output or []
            from airflow.utils import db
            db.initdb()
            import json
            kwargs = json.loads(kwargs_str)

            from datetime import datetime
            from airflow import DAG, settings
            from airflow.models import TaskInstance, Variable, XCom
            import logging
            import importlib
            import sys

            root = logging.getLogger()
            root.setLevel(logging.DEBUG)
            
            execution_date = datetime.now()            
            
            dag = DAG(dag_id='anydag', start_date=execution_date)
            Op = getattr(importlib.import_module("airflow.operators"), op_class)
            
            if 'python_callable' in kwargs:
                exec(kwargs['python_callable'])
                kwargs['python_callable'] = python_callable
            
            task = Op(dag=dag, task_id='anytask', **kwargs)
            ti = TaskInstance(task=task, execution_date=execution_date)
            result = task.execute(ti.get_template_context())            
 
            variables = {var.id: var.val for var in settings.Session().query(Variable).all()}
            xcoms = {msg.key: msg.value for msg in settings.Session().query(XCom).all()}

            output_values = {}

            if result_output_name is not None:
                output_values[result_output_name] = str(result)
            if variables_dict_output_name is not None:
                output_values[variables_dict_output_name] = json.dumps(variables)
            if xcoms_dict_output_name is not None:
                output_values[xcoms_dict_output_name] = json.dumps(xcoms)
            for name in variables_output_names:
                output_values[name] = variables[name]
            for name in xcoms_output_names:
                output_values[name] = xcoms[name]
                
            logging.info('Output: %s' % output_values)
            return output_values
        return _airflow

    # Hacking the function signature so that correct component interface is generated
    #import inspect
    #sig = inspect.Signature(
    #    parameters=inspect.signature(op_class).parameters.values(),
    #    #return_annotation=returnType,
    #)
    #_run_airflow_op_closure.__signature__ = sig

    return _func_to_component_spec(_run_airflow_op_closure(), base_image=base_image, modules_to_capture=modules_to_capture, component_name=task_id)
