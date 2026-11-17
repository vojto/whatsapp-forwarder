package main

// Expected behavior:
//   - skipReason decides at send time whether a chat is forwarded: groups with more than
//     maxGroupSize members and archived chats are skipped; everything else is forwarded.
//   - whatsmeowChats answers ChatInfo from the live whatsmeow client: archive state from the local
//     chat settings store, group name and size from the server.
//
// Gotchas:
//   - App state sync fails with "mismatching LTHash" for this account, so chat settings are never
//     populated and IsArchived is currently always false. A settings lookup error also counts as
//     not archived, so a broken store can't silently stop all forwards.
//   - GetGroupInfo is a server round trip; its errors are returned so the message is retried.

import (
	"context"
	"fmt"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
)

const maxGroupSize = 5

// ChatInfo is what the forwarding rules need to know about a chat at send time.
type ChatInfo struct {
	IsGroup    bool
	IsArchived bool
	GroupName  string
	GroupSize  int
}

// ChatLookup answers ChatInfo for a chat JID string (as stored in messages.chat_jid).
// Errors are treated as temporary (the message is retried with backoff).
type ChatLookup interface {
	ChatInfo(ctx context.Context, chatJID string) (ChatInfo, error)
}

type whatsmeowChats struct {
	client *whatsmeow.Client
}

// Lifecycle
// ---------------------------------------------------------------------------

func newWhatsmeowChats(client *whatsmeow.Client) *whatsmeowChats {
	return &whatsmeowChats{client: client}
}

func (chats *whatsmeowChats) ChatInfo(ctx context.Context, chatJID string) (ChatInfo, error) {
	jid, err := types.ParseJID(chatJID)
	if err != nil {
		return ChatInfo{}, fmt.Errorf("parse chat JID %q: %w", chatJID, err)
	}
	chat := ChatInfo{IsArchived: chats.isArchived(ctx, jid)}
	if jid.Server != types.GroupServer {
		return chat, nil
	}
	group, err := chats.client.GetGroupInfo(ctx, jid)
	if err != nil {
		return ChatInfo{}, fmt.Errorf("group info: %w", err)
	}
	chat.IsGroup = true
	chat.GroupName = group.Name
	chat.GroupSize = len(group.Participants)
	return chat, nil
}

// Rules
// ---------------------------------------------------------------------------

// skipReason returns why a message in this chat must not be forwarded, or "" to forward it.
func skipReason(chat ChatInfo) string {
	if chat.IsGroup && chat.GroupSize > maxGroupSize {
		return fmt.Sprintf("group of %d", chat.GroupSize)
	}
	if chat.IsArchived {
		return "archived chat"
	}
	return ""
}

// Chat state
// ---------------------------------------------------------------------------

func (chats *whatsmeowChats) isArchived(ctx context.Context, jid types.JID) bool {
	settings, err := chats.client.Store.ChatSettings.GetChatSettings(ctx, jid)
	return err == nil && settings.Archived
}
