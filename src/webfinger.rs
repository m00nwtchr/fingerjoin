use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

/// A single JRD link (RFC 7033 §4.4.4).
///
/// `rel` is required by the RFC, but kept optional here so that one malformed
/// link drops just that link during merge instead of failing the backend's
/// whole response. Unknown members (e.g. the nonstandard `template` used by
/// the OStatus subscribe rel) are preserved verbatim in `extra`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JrdLink {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rel: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub href: Option<String>,
    #[serde(rename = "type")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub titles: Option<HashMap<String, String>>,
    /// Property values are strings or null (RFC 7033 §4.4.4.4).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<HashMap<String, Option<String>>>,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct JrdResource {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub aliases: Vec<String>,
    /// Property values are strings or null (RFC 7033 §4.4.1).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub properties: HashMap<String, Option<String>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub links: Vec<JrdLink>,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

pub fn parse_jrd(bytes: &[u8]) -> Result<JrdResource, Error> {
    let resource: JrdResource = serde_json::from_slice(bytes)?;
    Ok(resource)
}

/// Merge JRD responses from multiple backends into one document.
///
/// Backends with a higher priority value win: the first non-empty subject is
/// kept, links are deduplicated by (`rel`, `href`) keeping the first seen, and
/// properties keep the first value per key. Aliases are concatenated and
/// deduplicated. Links without a `rel` are dropped (rel is required by RFC
/// 7033 §4.4.4.1).
pub fn merge_jrd(mut responses: Vec<(u16, JrdResource)>) -> JrdResource {
    responses.sort_by_key(|r| std::cmp::Reverse(r.0));

    let mut result = JrdResource::default();
    let mut seen_links: HashSet<(String, Option<String>)> = HashSet::new();
    let mut seen_aliases: HashSet<String> = HashSet::new();

    for (_, resp) in responses {
        if result.subject.is_none() {
            result.subject = resp.subject;
        }

        for alias in resp.aliases {
            if seen_aliases.insert(alias.clone()) {
                result.aliases.push(alias);
            }
        }

        for (k, v) in resp.properties {
            result.properties.entry(k).or_insert(v);
        }

        for link in resp.links {
            let Some(rel) = link.rel.clone() else {
                continue;
            };
            if seen_links.insert((rel, link.href.clone())) {
                result.links.push(link);
            }
        }

        for (k, v) in resp.extra {
            result.extra.entry(k).or_insert(v);
        }
    }

    result
}

/// Apply RFC 7033 §4.3 rel filtering: when the query carried one or more
/// `rel` parameters, only links whose `rel` matches one of them are returned.
/// Backends may or may not have applied the filter themselves, so it is
/// enforced here on the merged document.
pub fn filter_rels(resource: &mut JrdResource, rels: &[String]) {
    if rels.is_empty() {
        return;
    }
    resource.links.retain(|l| {
        l.rel
            .as_deref()
            .is_some_and(|r| rels.iter().any(|q| q == r))
    });
}

pub fn to_json_bytes(resource: &JrdResource) -> Result<Vec<u8>, Error> {
    serde_json::to_vec(resource).map_err(Error::Json)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn jrd(json: &str) -> JrdResource {
        parse_jrd(json.as_bytes()).expect("should parse")
    }

    #[test]
    fn test_parse_activitypub_response() {
        let j = jrd(
            r#"{"subject":"acct:user@example.com","aliases":["https://social.example.com/users/user","https://social.example.com/@user"],"links":[{"rel":"http://webfinger.net/rel/profile-page","type":"text/html","href":"https://social.example.com/@user"},{"rel":"self","type":"application/activity+json","href":"https://social.example.com/users/user"}]}"#,
        );
        assert_eq!(j.subject, Some("acct:user@example.com".to_string()));
        assert_eq!(j.aliases.len(), 2);
        assert_eq!(j.links.len(), 2);
        assert!(j.properties.is_empty());
    }

    #[test]
    fn test_parse_response_without_properties() {
        let j = jrd(
            r#"{"subject":"acct:registry@example.com","aliases":["https://registry.example.com/ap/actor"],"links":[{"rel":"self","type":"application/activity+json","href":"https://registry.example.com/ap/actor"}]}"#,
        );
        assert_eq!(j.subject, Some("acct:registry@example.com".to_string()));
        assert_eq!(j.aliases.len(), 1);
        assert_eq!(j.links.len(), 1);
    }

    #[test]
    fn test_parse_null_property_value() {
        // Null property values are legal (RFC 7033 §4.4.1) and used to
        // signal property removal; they must not fail the parse.
        let j = jrd(
            r#"{"subject":"acct:user@example.com","properties":{"http://example.com/ns/role":null}}"#,
        );
        assert_eq!(j.properties.get("http://example.com/ns/role"), Some(&None));
    }

    #[test]
    fn test_parse_preserves_unknown_link_members() {
        // Mastodon's subscribe link carries a nonstandard "template" member
        // that must survive the round trip.
        let j = jrd(
            r#"{"subject":"acct:user@example.com","links":[{"rel":"http://ostatus.org/schema/1.0/subscribe","template":"https://social.example.com/authorize_interaction?uri={uri}"}]}"#,
        );
        let out = String::from_utf8(to_json_bytes(&j).unwrap()).unwrap();
        assert!(out.contains(
            r#""template":"https://social.example.com/authorize_interaction?uri={uri}""#
        ));
    }

    #[test]
    fn test_merge_higher_priority_wins() {
        let low = jrd(
            r#"{"subject":"acct:low@example.com","properties":{"http://example.com/ns/p":"low"},"links":[{"rel":"self","href":"https://low.example.com/actor"}]}"#,
        );
        let high = jrd(
            r#"{"subject":"acct:high@example.com","properties":{"http://example.com/ns/p":"high"},"links":[{"rel":"self","href":"https://high.example.com/actor"}]}"#,
        );

        // Insertion order must not matter; the priority value must.
        let merged = merge_jrd(vec![(10, low), (100, high)]);
        assert_eq!(merged.subject, Some("acct:high@example.com".to_string()));
        assert_eq!(
            merged.properties.get("http://example.com/ns/p"),
            Some(&Some("high".to_string()))
        );
        // Different hrefs are distinct links, highest priority first.
        assert_eq!(merged.links.len(), 2);
        assert_eq!(
            merged.links[0].href.as_deref(),
            Some("https://high.example.com/actor")
        );
    }

    #[test]
    fn test_merge_dedups_links_and_aliases() {
        let a = jrd(
            r#"{"subject":"acct:user@example.com","aliases":["https://a.example.com/@user","https://shared.example.com/@user"],"links":[{"rel":"http://webfinger.net/rel/profile-page","href":"https://shared.example.com/@user"}]}"#,
        );
        let b = jrd(
            r#"{"aliases":["https://b.example.com/@user","https://shared.example.com/@user"],"links":[{"rel":"http://webfinger.net/rel/profile-page","href":"https://shared.example.com/@user","type":"text/html"}]}"#,
        );

        let merged = merge_jrd(vec![(50, a), (50, b)]);
        assert_eq!(merged.aliases.len(), 3);
        // Same (rel, href) from both backends collapses to one link.
        assert_eq!(merged.links.len(), 1);
    }

    #[test]
    fn test_merge_drops_links_without_rel() {
        let a = jrd(
            r#"{"subject":"acct:user@example.com","links":[{"href":"https://a.example.com/broken"},{"rel":"self","href":"https://a.example.com/actor"}]}"#,
        );
        let merged = merge_jrd(vec![(50, a)]);
        assert_eq!(merged.links.len(), 1);
        assert_eq!(merged.links[0].rel.as_deref(), Some("self"));
    }

    #[test]
    fn test_filter_rels() {
        let mut j = jrd(
            r#"{"subject":"acct:user@example.com","links":[{"rel":"self","href":"https://a.example.com/actor"},{"rel":"http://webfinger.net/rel/profile-page","href":"https://a.example.com/@user"}]}"#,
        );
        filter_rels(&mut j, &["self".to_string()]);
        assert_eq!(j.links.len(), 1);
        assert_eq!(j.links[0].rel.as_deref(), Some("self"));

        // No rel params: everything passes through untouched.
        let mut j2 = jrd(r#"{"links":[{"rel":"self","href":"https://a.example.com/actor"}]}"#);
        filter_rels(&mut j2, &[]);
        assert_eq!(j2.links.len(), 1);
    }

    #[test]
    fn test_serialize_omits_empty_members() {
        let out = String::from_utf8(to_json_bytes(&JrdResource::default()).unwrap()).unwrap();
        assert_eq!(out, "{}");
    }

    #[test]
    fn test_url_construction_with_resource() {
        let base = url::Url::parse("http://social.example.svc.cluster.local:8080").unwrap();
        let url = base.join("/.well-known/webfinger").unwrap();
        assert_eq!(
            url.as_str(),
            "http://social.example.svc.cluster.local:8080/.well-known/webfinger"
        );
    }
}
