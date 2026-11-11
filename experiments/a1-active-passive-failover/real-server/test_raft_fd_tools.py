"""Offline checks for the EC2 runner: no AWS calls or instance mutations."""
import base64
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from ec2_ssm import AWS
from local_raft_fd_validation import properties
from run_raft_fd_ssm import run


class RaftFdToolsTest(unittest.TestCase):
    def test_ssm_preserves_script_and_waits_for_terminal_success(self):
        aws=AWS(profile='test')
        script="echo 'line one'\necho 'line two'\n"
        aws.call=Mock(side_effect=[{'Command':{'CommandId':'cmd-test'}},
                                  {'Status':'InProgress'},
                                  {'Status':'Success','StandardOutputContent':'done'}])
        with patch('ec2_ssm.time.sleep'):
            self.assertEqual('done',aws.shell('i-abc',script))
        parameters=aws.call.call_args_list[0].args
        payload=json.loads(parameters[parameters.index('--parameters')+1])
        command=payload['commands'][0]
        self.assertIn(base64.b64encode(script.encode()).decode(),command)
        self.assertEqual(['120'],payload['executionTimeout'])
        self.assertEqual(3,aws.call.call_count)

    def test_ssm_failed_command_is_not_reported_as_success(self):
        aws=AWS()
        aws.call=Mock(side_effect=[{'Command':{'CommandId':'bad'}},
                                  {'Status':'Failed','StandardErrorContent':'failed'}])
        with self.assertRaisesRegex(RuntimeError,'bad'):
            aws.shell('i-abc','false')

    def test_invalid_instance_targets_fail_before_aws(self):
        with patch('run_raft_fd_ssm.AWS') as factory:
            with self.assertRaises(ValueError):
                run(SimpleNamespace(instances='i-abc,i-abc,i-def'))
            factory.assert_not_called()

    def test_auth_failure_happens_before_local_or_remote_writes(self):
        args=SimpleNamespace(instances='i-abc,i-def,i-fed',bucket='experiment-results',repeat=1,
                             timeout=500,value=64,aws_bin='aws',profile='test',region='ap-northeast-2')
        with patch('run_raft_fd_ssm.AWS') as factory:
            factory.return_value.call.side_effect=RuntimeError('AuthFailure')
            with self.assertRaisesRegex(RuntimeError,'AuthFailure'):
                run(args)
            factory.return_value.shell.assert_not_called()
            self.assertEqual(1,factory.return_value.call.call_count)

    def test_all_detectors_share_raft_configuration_and_node_mapping(self):
        hosts=['10.42.1.10','10.42.1.11','10.42.1.12']
        for node in range(3):
            for detector in ('FIXED_TIMEOUT','SWIM','SWIM_LIFEGUARD'):
                config=dict(line.split('=',1) for line in properties(hosts,node,detector,(7000,7001,7002),3).splitlines())
                self.assertEqual('RAFT',config['protocol'])
                self.assertEqual(str(node),config['node.id'])
                self.assertEqual(hosts[node],config[f'peer.{node}'])
                self.assertEqual(detector,config['failure.detector'])
                self.assertEqual('500',config['timeout.ms'])


if __name__=='__main__':
    unittest.main()
