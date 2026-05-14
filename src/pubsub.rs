use dashmap::DashMap;
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub enum PubSubMessage {
    Message {
        channel: String,
        message: String,
    },
    PMessage {
        pattern: String,
        channel: String,
        message: String,
    },
    Subscribe {
        channel: String,
        count: usize,
    },
    PSubscribe {
        pattern: String,
        count: usize,
    },
    Unsubscribe {
        channel: Option<String>,
        count: usize,
    },
    PUnsubscribe {
        pattern: Option<String>,
        count: usize,
    },
}

pub struct PubSub {
    channels: DashMap<String, ChannelSubscription>,
    patterns: DashMap<String, Vec<(String, mpsc::Sender<PubSubMessage>)>>,
}

pub struct ChannelSubscription {
    subscribers: DashMap<String, mpsc::Sender<PubSubMessage>>,
}

impl PubSub {
    pub fn new() -> Self {
        PubSub {
            channels: DashMap::new(),
            patterns: DashMap::new(),
        }
    }

    pub fn subscribe(
        &self,
        channel: &str,
        client_id: &str,
        sender: mpsc::Sender<PubSubMessage>,
    ) -> usize {
        let entry =
            self.channels
                .entry(channel.to_string())
                .or_insert_with(|| ChannelSubscription {
                    subscribers: DashMap::new(),
                });

        entry.subscribers.insert(client_id.to_string(), sender);
        entry.subscribers.len()
    }

    pub fn unsubscribe(&self, channel: &str, client_id: &str) -> Option<usize> {
        if let Some(entry) = self.channels.get_mut(channel) {
            entry.subscribers.remove(client_id);
            if entry.subscribers.is_empty() {
                self.channels.remove(channel);
            }
            Some(entry.subscribers.len())
        } else {
            None
        }
    }

    pub fn psubscribe(
        &self,
        pattern: &str,
        client_id: &str,
        sender: mpsc::Sender<PubSubMessage>,
    ) -> usize {
        let mut entry = self.patterns.entry(pattern.to_string()).or_default();
        entry.push((client_id.to_string(), sender));
        entry.len()
    }

    pub fn punsubscribe(&self, pattern: &str, client_id: &str) -> Option<usize> {
        if let Some(mut entry) = self.patterns.get_mut(pattern) {
            entry.retain(|(id, _)| id != client_id);
            if entry.is_empty() {
                self.patterns.remove(pattern);
            }
            Some(entry.len())
        } else {
            None
        }
    }

    pub fn publish(&self, channel: &str, message: &str) -> usize {
        let mut total = 0;

        if let Some(entry) = self.channels.get(channel) {
            let count = entry.subscribers.len();
            total += count;

            let msg = PubSubMessage::Message {
                channel: channel.to_string(),
                message: message.to_string(),
            };

            let clients_to_remove: Vec<_> = entry
                .subscribers
                .iter()
                .filter(|sub| sub.value().try_send(msg.clone()).is_err())
                .map(|sub| sub.key().clone())
                .collect();

            for client_id in clients_to_remove {
                entry.subscribers.remove(&client_id);
            }
        }

        for entry in self.patterns.iter() {
            let pattern = entry.key();
            if glob_match(channel.as_bytes(), pattern.as_bytes()) {
                total += entry.value().len();

                for (_client_id, sender) in entry.value().iter() {
                    let pm = PubSubMessage::PMessage {
                        pattern: pattern.to_string(),
                        channel: channel.to_string(),
                        message: message.to_string(),
                    };
                    if sender.try_send(pm.clone()).is_err() {
                        total -= 1;
                    }
                }
            }
        }

        total
    }

    pub fn channels(&self, pattern: Option<&str>) -> Vec<String> {
        match pattern {
            Some(p) => self
                .channels
                .iter()
                .filter(|ch| glob_match(ch.key().as_bytes(), p.as_bytes()))
                .map(|ch| ch.key().clone())
                .collect(),
            None => self.channels.iter().map(|ch| ch.key().clone()).collect(),
        }
    }

    pub fn numsub(&self, channels: &[&str]) -> Vec<(String, usize)> {
        channels
            .iter()
            .map(|ch| {
                let ch_string = ch.to_string();
                let count = self
                    .channels
                    .get(&ch_string)
                    .map(|e| e.subscribers.len())
                    .unwrap_or(0);
                (ch_string, count)
            })
            .collect()
    }

    pub fn numpat(&self) -> usize {
        self.patterns.iter().map(|e| e.value().len()).sum()
    }

    pub fn channels_count(&self) -> usize {
        self.channels.len()
    }

    pub fn patterns_count(&self) -> usize {
        self.patterns.len()
    }

    pub fn remove_client(&self, client_id: &str, channels: &[String], patterns: &[String]) {
        for ch in channels {
            if let Some(entry) = self.channels.get_mut(ch) {
                entry.subscribers.remove(client_id);
                if entry.subscribers.is_empty() {
                    self.channels.remove(ch);
                }
            }
        }
        for pat in patterns {
            if let Some(mut entry) = self.patterns.get_mut(pat) {
                entry.retain(|(id, _)| id != client_id);
                if entry.is_empty() {
                    self.patterns.remove(pat);
                }
            }
        }
    }

    pub fn subscription_count(
        &self,
        client_id: &str,
        channels: &[String],
        patterns: &[String],
    ) -> (usize, usize) {
        let ch_count = channels
            .iter()
            .filter(|ch| {
                self.channels
                    .get(*ch)
                    .map(|e| e.subscribers.contains_key(client_id))
                    .unwrap_or(false)
            })
            .count();
        let pat_count = patterns
            .iter()
            .filter(|pat| {
                self.patterns
                    .get(*pat)
                    .map(|e| e.value().iter().any(|(id, _)| id == client_id))
                    .unwrap_or(false)
            })
            .count();
        (ch_count, pat_count)
    }
}

impl Default for PubSub {
    fn default() -> Self {
        Self::new()
    }
}

fn glob_match(s: &[u8], p: &[u8]) -> bool {
    match (s.first(), p.first()) {
        (_, Some(b'*')) => glob_match(s, &p[1..]) || (!s.is_empty() && glob_match(&s[1..], p)),
        (Some(sc), Some(pc)) => (*pc == b'?' || sc == pc) && glob_match(&s[1..], &p[1..]),
        (None, None) => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_pubsub() -> PubSub {
        PubSub::new()
    }

    #[test]
    fn test_publish_no_subscriber() {
        let ps = new_pubsub();
        let count = ps.publish("news", "hello");
        assert_eq!(count, 0);
    }

    #[test]
    fn test_psubscribe() {
        let ps = new_pubsub();
        let (tx1, _rx1) = mpsc::channel(100);
        let (tx2, _rx2) = mpsc::channel(100);

        let count1 = ps.psubscribe("news.*", "client1", tx1);
        assert_eq!(count1, 1);

        let count2 = ps.psubscribe("news.*", "client2", tx2);
        assert_eq!(count2, 2);
    }

    #[test]
    fn test_psubscribe_and_publish() {
        let ps = new_pubsub();
        let (tx, _rx) = mpsc::channel(100);

        ps.psubscribe("news.*", "client1", tx);
        let count = ps.publish("news.sports", "score update");
        assert_eq!(count, 1);
    }

    #[test]
    fn test_psubscribe_no_match() {
        let ps = new_pubsub();
        let (tx, _rx) = mpsc::channel(100);

        ps.psubscribe("news.*", "client1", tx);
        let count = ps.publish("weather.rain", "rain expected");
        assert_eq!(count, 0);
    }

    #[test]
    fn test_pubsub_channels() {
        let ps = new_pubsub();
        let (tx, _rx) = mpsc::channel(100);
        ps.subscribe("news", "c1", tx);
        let (tx2, _rx2) = mpsc::channel(100);
        ps.subscribe("sports", "c2", tx2);

        let all = ps.channels(None);
        assert!(all.contains(&"news".to_string()));
        assert!(all.contains(&"sports".to_string()));

        let news_only = ps.channels(Some("news*"));
        assert!(news_only.contains(&"news".to_string()));
    }

    #[test]
    fn test_pubsub_numsub() {
        let ps = new_pubsub();
        let (tx1, _rx) = mpsc::channel(100);
        ps.subscribe("news", "c1", tx1);
        let (tx2, _rx2) = mpsc::channel(100);
        ps.subscribe("news", "c2", tx2);
        let (tx3, _rx3) = mpsc::channel(100);
        ps.subscribe("sports", "c3", tx3);

        let counts = ps.numsub(&["news", "sports", "weather"]);
        assert_eq!(counts[0], ("news".to_string(), 2));
        assert_eq!(counts[1], ("sports".to_string(), 1));
        assert_eq!(counts[2], ("weather".to_string(), 0));
    }

    #[test]
    fn test_pubsub_numpat() {
        let ps = new_pubsub();
        let (tx1, _rx) = mpsc::channel(100);
        ps.psubscribe("news.*", "c1", tx1);
        let (tx2, _rx2) = mpsc::channel(100);
        ps.psubscribe("weather.*", "c2", tx2);

        assert_eq!(ps.numpat(), 2);
    }

    #[test]
    fn test_multiple_subscribers_same_channel() {
        let ps = new_pubsub();
        let (tx1, _rx1) = mpsc::channel(100);
        ps.subscribe("broad", "c1", tx1);
        let (tx2, _rx2) = mpsc::channel(100);
        ps.subscribe("broad", "c2", tx2);
        let (tx3, _rx3) = mpsc::channel(100);
        ps.subscribe("broad", "c3", tx3);

        let count = ps.publish("broad", "broadcast message");
        assert_eq!(count, 3);
    }

    #[test]
    fn test_channels_count() {
        let ps = new_pubsub();
        let (tx, _rx) = mpsc::channel(100);
        ps.subscribe("news", "c1", tx);

        assert_eq!(ps.channels_count(), 1);
    }

    #[test]
    fn test_patterns_count() {
        let ps = new_pubsub();
        let (tx, _rx) = mpsc::channel(100);
        ps.psubscribe("news.*", "c1", tx);

        assert_eq!(ps.patterns_count(), 1);
    }
}
