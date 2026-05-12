use {
    crate::metrics::REGISTRY, bytes::Bytes, http_body_util::Full,
    hyper::Response, log::*, prometheus::TextEncoder,
};

pub fn metrics_handler() -> Response<Full<Bytes>> {
    let metrics = TextEncoder::new()
        .encode_to_string(&REGISTRY.gather())
        .unwrap_or_else(|error| {
            error!("could not encode custom metrics: {}", error);
            String::new()
        });
    match Response::builder().body(Full::new(Bytes::from(metrics))) {
        Ok(response) => response,
        Err(error) => {
            error!("failed to build metrics response: {error}");
            Response::new(Full::new(Bytes::new()))
        }
    }
}
