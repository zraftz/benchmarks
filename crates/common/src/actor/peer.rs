//! Drain only the contiguous ready prefix. A boundary stays first for the next turn.
use super::*;

#[allow(clippy::too_many_arguments)]
pub(super) fn collect<E: Engine>(
    engine: &E,
    from: u64,
    data: Vec<u8>,
    cap: usize,
    rx: &mpsc::Receiver<Input>,
    deferred: &mut VecDeque<Input>,
    diagnostics: &Diagnostics,
    arrivals: &mut Vec<Option<Instant>>,
) -> Result<Vec<E::Peer>> {
    let mut gate = engine.peer_gate(cap, 256 * 1024);
    let first = engine.decode_peer(from, &data)?;
    let admitted = E::admit_peer(&mut gate, &first);
    let mut peers = vec![first];
    if admitted {
        while peers.len() < cap {
            let Some(input) = deferred.pop_front().or_else(|| rx.try_recv().ok()) else {
                break;
            };
            if let Input::Peer(from, data, queued) = &input {
                let peer = engine.decode_peer(*from, data)?;
                if E::admit_peer(&mut gate, &peer) {
                    diagnostics.elapsed("owner_peer_queue_ns", *queued);
                    arrivals.push(*queued);
                    peers.push(peer);
                    continue;
                }
            }
            deferred.push_front(input);
            break;
        }
    }
    Ok(peers)
}

#[cfg(test)]
#[path = "peer_test.rs"]
mod tests;
