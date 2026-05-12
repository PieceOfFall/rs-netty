#[test]
fn compile_fail() {
    let t = trybuild::TestCases::new();

    t.compile_fail("tests/ui/fail_inbound_before_codec.rs");
    t.compile_fail("tests/ui/fail_outbound_before_handler.rs");
    t.compile_fail("tests/ui/fail_handler_before_codec.rs");
    t.compile_fail("tests/ui/fail_handler_twice.rs");
    t.compile_fail("tests/ui/fail_inbound_after_handler.rs");
    t.compile_fail("tests/ui/fail_type_mismatch_inbound_to_handler.rs");
    t.compile_fail("tests/ui/fail_type_mismatch_outbound.rs");
    t.compile_fail("tests/ui/fail_final_encoder_mismatch.rs");
}
