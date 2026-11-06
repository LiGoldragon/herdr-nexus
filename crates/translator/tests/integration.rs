#![cfg(feature = "text")]
use datom_codec::{Actualizing, Budget, Potential};
use signal_herdr_nexus::{Outcome, Query, Response};
use shared_translator::{ByteViewable, Restorable, Signal, Signalizable};

#[test]
fn public_query_text_binary_response_text_crosses_layers() {
    let input = "List.main";
    let mut budget = Budget { remaining: 32, reader: protos::ReaderBudget::try_from(32).unwrap(), depth: 0, maximum_depth: 8 };
    let mut potential = Potential::<Query>::from(input);
    let query = potential.actualize(&mut budget).unwrap();
    let wire = query.signalize().unwrap();
    let restored = Signal::<Query>::from(wire.bytes().to_vec()).restore().unwrap();
    assert_eq!(restored, query);
    let response = Response::Listed(Outcome::Listed(vec![]));
    assert_eq!(shared_translator::encode(&response), "Listed.Listed.[]");
}
