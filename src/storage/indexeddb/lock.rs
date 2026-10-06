use crate::errors::{Error, Result};
use tokio::sync::{mpsc, oneshot};
use wasm_bindgen::closure::Closure;
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::{future_to_promise, spawn_local, JsFuture};
use web_sys::js_sys::{global, Function, Promise, Reflect};

/// An exclusive Web Lock, held until this value is dropped.
pub(super) struct Lock {
    _release: oneshot::Sender<()>,
}

/// Wait for the exclusive Web Lock with the given name.
pub(super) async fn acquire(name: String) -> Result<Lock> {
    let (granted_tx, mut granted_rx) = mpsc::unbounded_channel();
    let (release, released) = oneshot::channel::<()>();
    let lock_name = name.clone();
    // The browser holds the lock until the promise the callback returns settles, and that
    // promise settles when `release` is sent or dropped.
    spawn_local(async move {
        let on_grant = granted_tx.clone();
        let callback = Closure::once(move |_: JsValue| -> Promise {
            let _ = on_grant.send(Ok(()));
            future_to_promise(async move {
                let _ = released.await;
                Ok(JsValue::UNDEFINED)
            })
        });
        let request = || -> std::result::Result<Promise, JsValue> {
            let navigator = Reflect::get(&global(), &"navigator".into())?;
            let locks = Reflect::get(&navigator, &"locks".into())?;
            let request: Function = Reflect::get(&locks, &"request".into())?.dyn_into()?;
            request
                .call2(&locks, &lock_name.as_str().into(), callback.as_ref())?
                .dyn_into()
        };
        let held = match request() {
            Ok(held) => JsFuture::from(held).await,
            Err(e) => Err(e),
        };
        if let Err(e) = held {
            let _ = granted_tx.send(Err(format!("{e:?}")));
        }
    });
    match granted_rx.recv().await {
        Some(Ok(())) => Ok(Lock { _release: release }),
        Some(Err(e)) => Err(Error::Database(format!("Web Lock {name}: {e}"))),
        None => Err(Error::Database(format!("Web Lock {name} was not granted"))),
    }
}
