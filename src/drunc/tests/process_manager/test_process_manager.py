# just a placeholder for testing impls of describe and flush
# which don't depend on the concrete implementaiton of process manager abstract class
if False:

    def test_describe_endpoint(grpc_test_server_no_impl):
        """
        Test that invoking the describe method gives the expected response.

        Validates that the describe endpoint correctly processes generic requests
        and returns service description information.

        Args:
            grpc_test_server_no_impl: Tuple containing test server and servicer instance
        """
        from drunc.tests.process_manager.dummy_requests import GENERIC_REQUEST

        process_manager: ProcessManager
        (grpc_test_server, process_manager) = grpc_test_server_no_impl

        process_manager.get_log_path = MagicMock(return_value="mock_log_path")

        expected_response = Description(
            type="process_manager",
            name="process_manager_no_impl",
            info=process_manager.get_log_path(),
            session="mock_session",
            commands=process_manager.commands,
            children=[],
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
            token=None,
        )

        # Invoke the describe method via gRPC testing framework
        method = grpc_test_server.invoke_unary_unary(
            method_descriptor=(
                DESCRIPTOR.services_by_name["ProcessManager"].methods_by_name[
                    "describe"
                ]
            ),
            invocation_metadata={},
            request=GENERIC_REQUEST,
            timeout=1,
        )

        # Block until response is ready and extract all response components
        response, metadata, code, details = method.termination()

        # Verify the RPC completed successfully
        assert code == grpc.StatusCode.OK

        # Verify all response fields match expected values
        assert expected_response == response
