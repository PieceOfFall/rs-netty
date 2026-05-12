#[test]
fn compile_pass() {
    let t = trybuild::TestCases::new();
    t.pass("tests/ui/pass_echo.rs");
    t.pass("tests/ui/pass_typed_chain.rs");
}
