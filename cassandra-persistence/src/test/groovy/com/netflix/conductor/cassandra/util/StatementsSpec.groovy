/*
 * Copyright 2022 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.cassandra.util

import spock.lang.Specification
import spock.lang.Subject

class StatementsSpec extends Specification {

    @Subject
    Statements subject

    def setup() {
        subject = new Statements('test')
    }

    def "verify statements"() {
        when:
        subject

        then:
        with(subject) {
            "INSERT INTO test.workflow_definitions (workflow_def_name,version,workflow_definition) VALUES (?,?,?) IF NOT EXISTS;".equals(insertWorkflowDefStatement)
            "INSERT INTO test.task_definitions (task_defs,task_def_name,task_definition) VALUES ('task_defs',?,?);".equals(insertTaskDefStatement)
            "SELECT workflow_definition FROM test.workflow_definitions WHERE workflow_def_name=? AND version=?;".equals(selectWorkflowDefStatement)
            "SELECT * FROM test.workflow_definitions WHERE workflow_def_name=?;".equals(selectAllWorkflowDefVersionsByNameStatement)
            "SELECT * FROM test.workflow_defs_index WHERE workflow_def_version_index=?;".equals(selectAllWorkflowDefsStatement)
            "SELECT task_definition FROM test.task_definitions WHERE task_defs='task_defs' AND task_def_name=?;".equals(selectTaskDefStatement)
            "SELECT * FROM test.task_definitions WHERE task_defs=?;".equals(selectAllTaskDefsStatement)
            "UPDATE test.workflow_definitions SET workflow_definition=? WHERE workflow_def_name=? AND version=?;".equals(updateWorkflowDefStatement)
            "DELETE FROM test.workflow_definitions WHERE workflow_def_name=? AND version=?;".equals(deleteWorkflowDefStatement)
            "DELETE FROM test.workflow_defs_index WHERE workflow_def_version_index=? AND workflow_def_name_version=?;".equals(deleteWorkflowDefIndexStatement)
            "DELETE FROM test.task_definitions WHERE task_defs='task_defs' AND task_def_name=?;".equals(deleteTaskDefStatement)
            "INSERT INTO test.workflows (workflow_id,shard_id,task_id,entity,payload,total_tasks,total_partitions) VALUES (?,?,?,'workflow',?,?,?);".equals(insertWorkflowStatement)
            "INSERT INTO test.workflows (workflow_id,shard_id,task_id,entity,payload) VALUES (?,?,?,'task',?);".equals(insertTaskStatement)
            "INSERT INTO test.event_executions (message_id,event_handler_name,event_execution_id,payload) VALUES (?,?,?,?) IF NOT EXISTS;".equals(insertEventExecutionStatement)
            "SELECT total_tasks,total_partitions FROM test.workflows WHERE workflow_id=? AND shard_id=1;".equals(selectTotalStatement)
            "SELECT payload FROM test.workflows WHERE workflow_id=? AND shard_id=? AND entity='task' AND task_id=?;".equals(selectTaskStatement)
            "SELECT payload FROM test.workflows WHERE workflow_id=? AND shard_id=1 AND entity='workflow';".equals(selectWorkflowStatement)
            "SELECT * FROM test.workflows WHERE workflow_id=? AND shard_id=?;".equals(selectWorkflowWithTasksStatement)
            "SELECT workflow_id FROM test.task_lookup WHERE task_id=?;".equals(selectTaskFromLookupTableStatement)
            "SELECT * FROM test.task_def_limit WHERE task_def_name=?;".equals(selectTasksFromTaskDefLimitStatement)
            "SELECT * FROM test.event_executions WHERE message_id=? AND event_handler_name=?;".equals(selectAllEventExecutionsForMessageFromEventExecutionsStatement)
            "UPDATE test.workflows SET payload=? WHERE workflow_id=? AND shard_id=1 AND entity='workflow' AND task_id='';".equals(updateWorkflowStatement)
            "UPDATE test.workflows SET total_tasks=? WHERE workflow_id=? AND shard_id=?;".equals(updateTotalTasksStatement)
            "UPDATE test.workflows SET total_partitions=?,total_tasks=? WHERE workflow_id=? AND shard_id=1;".equals(updateTotalPartitionsStatement)
            "UPDATE test.task_lookup SET workflow_id=? WHERE task_id=?;".equals(updateTaskLookupStatement)
            "UPDATE test.task_def_limit SET workflow_id=? WHERE task_def_name=? AND task_id=?;".equals(updateTaskDefLimitStatement)
            "UPDATE test.event_executions USING TTL ? SET payload=? WHERE message_id=? AND event_handler_name=? AND event_execution_id=?;".equals(updateEventExecutionStatement)
            "DELETE FROM test.workflows WHERE workflow_id=? AND shard_id=?;".equals(deleteWorkflowStatement)
            "DELETE FROM test.task_lookup WHERE task_id=?;".equals(deleteTaskLookupStatement)
            "DELETE FROM test.workflows WHERE workflow_id=? AND shard_id=? AND entity='task' AND task_id=?;".equals(deleteTaskStatement)
            "DELETE FROM test.task_def_limit WHERE task_def_name=? AND task_id=?;".equals(deleteTaskDefLimitStatement)
            "DELETE FROM test.event_executions WHERE message_id=? AND event_handler_name=? AND event_execution_id=?;".equals(deleteEventExecutionsStatement)
            "INSERT INTO test.event_handlers (handlers,event_handler_name,event_handler) VALUES ('handlers',?,?);".equals(insertEventHandlerStatement)
            "SELECT * FROM test.event_handlers WHERE handlers=?;".equals(selectAllEventHandlersStatement)
            "DELETE FROM test.event_handlers WHERE handlers='handlers' AND event_handler_name=?;".equals(deleteEventHandlerStatement)
        }
    }
}
