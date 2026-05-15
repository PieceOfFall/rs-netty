#[test]
fn compile_pass() {
    let t = trybuild::TestCases::new();
    t.pass("tests/ui/pass_tcp_echo.rs");
    t.pass("tests/ui/pass_tcp_client.rs");
    t.pass("tests/ui/pass_tcp_client_pipeline_instance.rs");
    t.pass("tests/ui/pass_tcp_typed_chain.rs");
    t.pass("tests/ui/pass_udp_echo.rs");
    t.pass("tests/ui/pass_udp_client.rs");
    t.pass("tests/ui/pass_udp_typed_chain.rs");
    t.pass("tests/ui/pass_life.rs");
    t.pass("tests/ui/pass_client_server_modules.rs");
    t.pass("tests/ui/pass_http_codec.rs");

    #[cfg(feature = "macros")]
    t.pass("tests/ui/pass_tcp_handler_macro.rs");

    #[cfg(feature = "websocket")]
    t.pass("tests/ui/pass_http_ws_router.rs");
}
