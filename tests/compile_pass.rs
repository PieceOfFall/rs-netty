#[test]
fn compile_pass() {
    let t = trybuild::TestCases::new();
    t.pass("tests/ui/pass_tcp_echo.rs");
    t.pass("tests/ui/pass_tcp_client.rs");
    t.pass("tests/ui/pass_tcp_typed_chain.rs");
    t.pass("tests/ui/pass_udp_echo.rs");
    t.pass("tests/ui/pass_udp_client.rs");
    t.pass("tests/ui/pass_udp_typed_chain.rs");
    t.pass("tests/ui/pass_life.rs");
}
