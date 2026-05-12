#[test]
fn compile_fail() {
    let t = trybuild::TestCases::new();

    t.compile_fail("tests/ui/fail_stream_inbound_before_codec.rs");
    t.compile_fail("tests/ui/fail_stream_outbound_before_handler.rs");
    t.compile_fail("tests/ui/fail_stream_handler_before_codec.rs");
    t.compile_fail("tests/ui/fail_stream_handler_twice.rs");
    t.compile_fail("tests/ui/fail_stream_inbound_after_handler.rs");
    t.compile_fail("tests/ui/fail_stream_type_mismatch_inbound_to_handler.rs");
    t.compile_fail("tests/ui/fail_stream_type_mismatch_outbound.rs");
    t.compile_fail("tests/ui/fail_stream_final_encoder_mismatch.rs");

    t.compile_fail("tests/ui/fail_datagram_inbound_before_codec.rs");
    t.compile_fail("tests/ui/fail_datagram_outbound_before_handler.rs");
    t.compile_fail("tests/ui/fail_datagram_handler_before_codec.rs");
    t.compile_fail("tests/ui/fail_datagram_handler_twice.rs");
    t.compile_fail("tests/ui/fail_datagram_inbound_after_handler.rs");
    t.compile_fail("tests/ui/fail_datagram_type_mismatch_inbound_to_handler.rs");
    t.compile_fail("tests/ui/fail_datagram_type_mismatch_outbound.rs");
    t.compile_fail("tests/ui/fail_datagram_final_encoder_mismatch.rs");

    t.compile_fail("tests/ui/fail_udp_with_stream_pipeline.rs");
    t.compile_fail("tests/ui/fail_tcp_with_datagram_pipeline.rs");
}
