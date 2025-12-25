use super::*;

pub struct CopyMembership;

impl CopyMembership {
    pub async fn copy(st: impl Stream<Item = sorock::Membership>, nodes: Arc<RwLock<Nodes>>) {
        let mut st = Box::pin(st);
        while let Some(membership) = st.next().await {
            let new_membership = {
                let mut out = HashSet::new();
                for (member, _) in membership.members {
                    let url = Uri::from_maybe_shared(member).unwrap();
                    out.insert(url);
                }
                out
            };

            let mut nodes = nodes.write();
            nodes.update_membership(new_membership).await;
        }
    }
}
