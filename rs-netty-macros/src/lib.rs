#![deny(unsafe_code)]

use proc_macro::TokenStream;
use quote::{quote, ToTokens};
use syn::{
    parse::{Parse, ParseStream},
    parse_macro_input, AngleBracketedGenericArguments, FnArg, GenericArgument, Ident, ItemFn, Path,
    PathArguments, ReturnType, Token, Type,
};

/// Adapts an async function into TCP and UDP handler implementations.
///
/// The attribute is intended for simple final handlers whose logic fits in an
/// async function. The handler type is still declared by the user, which keeps
/// IDE navigation and type names explicit, while the macro generates the
/// repetitive `Handler` and `DatagramHandler` impls.
///
/// # Request to response handlers
///
/// When the function returns `Result<Out>`, the macro sets `type Write = Out`
/// and writes the returned value through the handler context.
///
/// ```ignore
/// struct Echo;
///
/// #[handler(Echo)]
/// async fn echo(msg: String) -> rs_netty::Result<String> {
///     Ok(msg)
/// }
/// ```
///
/// This expands to an implementation roughly equivalent to:
///
/// ```ignore
/// impl rs_netty::Handler<String> for Echo {
///     type Write = String;
///
///     async fn read(
///         &mut self,
///         ctx: &mut rs_netty::Context<Self::Write>,
///         msg: String,
///     ) -> rs_netty::Result<()> {
///         let msg = echo(msg).await?;
///         ctx.write(msg).await
///     }
/// }
/// ```
///
/// # Consume-only handlers
///
/// When the function returns `Result<()>`, the macro cannot infer
/// `Handler::Write` from the return type. Use `write = Type` to state what the
/// connection can write from outside, through `TcpClientHandle::write`,
/// `write_and_flush`, or the handler context.
///
/// ```ignore
/// struct Request;
/// struct Response {
///     message: String,
/// }
///
/// struct PrintResponse;
///
/// #[handler(PrintResponse, write = Request)]
/// async fn print_response(res: Response) -> rs_netty::Result<()> {
///     println!("{}", res.message);
///     Ok(())
/// }
/// ```
///
/// # Accessing handler state
///
/// Add `&mut HandlerType` as the first function argument when the function
/// needs to mutate fields on the handler value. This is useful for one-shot
/// notifications, counters, or other per-connection state.
///
/// ```ignore
/// struct Response;
///
/// struct WaitForResponse {
///     done: Option<tokio::sync::oneshot::Sender<()>>,
/// }
///
/// #[handler(WaitForResponse, write = String)]
/// async fn wait_for_response(
///     handler: &mut WaitForResponse,
///     _res: Response,
/// ) -> rs_netty::Result<()> {
///     if let Some(done) = handler.done.take() {
///         let _ = done.send(());
///     }
///     Ok(())
/// }
/// ```
///
/// # When to write the impl by hand
///
/// Use a manual `impl Handler` or `impl DatagramHandler` when the handler needs
/// direct access to `Context`, `DatagramContext`, explicit flush timing,
/// multiple writes per read, or APIs not represented by the function forms
/// above.
#[proc_macro_attribute]
pub fn handler(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as HandlerArgs);
    let function = parse_macro_input!(item as ItemFn);

    expand_handler(args, function)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

struct HandlerArgs {
    handler_ty: Path,
    write_ty: Option<Type>,
}

impl Parse for HandlerArgs {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let handler_ty = input.parse::<Path>()?;
        let mut write_ty = None;

        if input.peek(Token![,]) {
            input.parse::<Token![,]>()?;
            let ident = input.parse::<Ident>()?;
            if ident != "write" {
                return Err(syn::Error::new_spanned(ident, "expected `write = Type`"));
            }
            input.parse::<Token![=]>()?;
            write_ty = Some(input.parse::<Type>()?);
        }

        if !input.is_empty() {
            return Err(input.error("unexpected tokens in `#[handler]`"));
        }

        Ok(Self {
            handler_ty,
            write_ty,
        })
    }
}

fn expand_handler(args: HandlerArgs, function: ItemFn) -> syn::Result<proc_macro2::TokenStream> {
    if function.sig.asyncness.is_none() {
        return Err(syn::Error::new_spanned(
            function.sig.fn_token,
            "`#[handler]` can only be used on async functions",
        ));
    }

    let handler_ty = args.handler_ty;
    let signature = handler_signature(&function)?;
    let input_ty = signature.message_ty;
    let ok_ty = result_ok_type(&function.sig.output)?;
    let writes_response = !is_unit_type(&ok_ty);
    let write_ty = match (args.write_ty, writes_response) {
        (Some(write_ty), false) => write_ty,
        (Some(write_ty), true) => {
            return Err(syn::Error::new_spanned(
                write_ty,
                "`write = Type` is only supported for handlers that return Result<()>",
            ));
        }
        (None, true) => ok_ty.clone(),
        (None, false) => {
            return Err(syn::Error::new_spanned(
                &function.sig.output,
                "`#[handler]` functions that return Result<()> must specify `write = Type`",
            ));
        }
    };
    let fn_name = &function.sig.ident;
    let call = if signature.takes_state {
        quote! { #fn_name(self, msg).await? }
    } else {
        quote! { #fn_name(msg).await? }
    };
    let tcp_body = if writes_response {
        quote! {
            let msg = #call;
            ctx.write(msg).await
        }
    } else {
        quote! {
            #call;
            let _ = ctx;
            Ok(())
        }
    };
    let datagram_body = if writes_response {
        quote! {
            let msg = #call;
            ctx.write(msg).await
        }
    } else {
        quote! {
            #call;
            let _ = ctx;
            Ok(())
        }
    };

    Ok(quote! {
        #function

        impl ::rs_netty::Handler<#input_ty> for #handler_ty {
            type Write = #write_ty;

            async fn read(
                &mut self,
                ctx: &mut ::rs_netty::Context<Self::Write>,
                msg: #input_ty,
            ) -> ::rs_netty::Result<()> {
                #tcp_body
            }
        }

        impl ::rs_netty::DatagramHandler<#input_ty> for #handler_ty {
            type Write = #write_ty;

            async fn read(
                &mut self,
                ctx: &mut ::rs_netty::DatagramContext<Self::Write>,
                msg: #input_ty,
            ) -> ::rs_netty::Result<()> {
                #datagram_body
            }
        }
    })
}

struct HandlerSignature {
    takes_state: bool,
    message_ty: Type,
}

fn handler_signature(function: &ItemFn) -> syn::Result<HandlerSignature> {
    let mut inputs = function.sig.inputs.iter();
    let Some(input) = inputs.next() else {
        return Err(syn::Error::new_spanned(
            &function.sig.ident,
            "`#[handler]` functions must accept a message argument",
        ));
    };

    let first = typed_input(input)?;
    if is_mut_ref_type(first.ty.as_ref()) {
        let Some(input) = inputs.next() else {
            return Err(syn::Error::new_spanned(
                first,
                "`#[handler]` functions with a state argument must also accept a message argument",
            ));
        };
        let message = typed_input(input)?;
        if let Some(extra) = inputs.next() {
            return Err(syn::Error::new_spanned(
                extra,
                "`#[handler]` functions can accept at most a state argument and a message argument",
            ));
        }

        return Ok(HandlerSignature {
            takes_state: true,
            message_ty: (*message.ty).clone(),
        });
    }

    if let Some(extra) = inputs.next() {
        return Err(syn::Error::new_spanned(
            extra,
            "`#[handler]` functions can accept at most a state argument and a message argument",
        ));
    }

    Ok(HandlerSignature {
        takes_state: false,
        message_ty: (*first.ty).clone(),
    })
}

fn typed_input(input: &FnArg) -> syn::Result<&syn::PatType> {
    match input {
        FnArg::Typed(input) => Ok(input),
        FnArg::Receiver(receiver) => Err(syn::Error::new_spanned(
            receiver,
            "`#[handler]` functions cannot take a self receiver",
        )),
    }
}

fn result_ok_type(output: &ReturnType) -> syn::Result<Type> {
    let ReturnType::Type(_, ty) = output else {
        return Err(syn::Error::new_spanned(
            output,
            "`#[handler]` functions must return Result<Write>",
        ));
    };

    let Type::Path(type_path) = ty.as_ref() else {
        return Err(syn::Error::new_spanned(
            ty,
            "`#[handler]` functions must return Result<Write>",
        ));
    };

    let Some(segment) = type_path.path.segments.last() else {
        return Err(syn::Error::new_spanned(
            ty,
            "`#[handler]` functions must return Result<Write>",
        ));
    };

    if segment.ident != "Result" {
        return Err(syn::Error::new_spanned(
            ty,
            "`#[handler]` functions must return Result<Write>",
        ));
    }

    let PathArguments::AngleBracketed(AngleBracketedGenericArguments { args, .. }) =
        &segment.arguments
    else {
        return Err(syn::Error::new_spanned(
            ty,
            "`#[handler]` functions must return Result<Write>",
        ));
    };

    match args.first() {
        Some(GenericArgument::Type(ok_ty)) => Ok(ok_ty.clone()),
        _ => Err(syn::Error::new_spanned(
            args.to_token_stream(),
            "`#[handler]` functions must return Result<Write>",
        )),
    }
}

fn is_mut_ref_type(ty: &Type) -> bool {
    matches!(ty, Type::Reference(reference) if reference.mutability.is_some())
}

fn is_unit_type(ty: &Type) -> bool {
    matches!(ty, Type::Tuple(tuple) if tuple.elems.is_empty())
}
