import { EventType } from "@ag-ui/core";

/**
 * Folding an AG-UI event stream into the conversation it produced.
 *
 * The hosted Intelligence platform persists events per run and serves
 * `GET /api/threads/:id/messages` as the folded transcript; this is the same fold.
 * Runs replay in order, so a single left fold over the ordered event log
 * reproduces the transcript the browser rendered.
 *
 * Scope is what the chat renders: text messages, tool calls (as assistant
 * `toolCalls` and `tool` results), and `MESSAGES_SNAPSHOT` as a wholesale
 * replacement. Run bookends, state events and step events do not touch messages.
 * The rows are JSON, so the fold is typed loosely; the events driving it are the
 * AG-UI event union.
 */

export type FoldMessage = {
  id: string;
  role: string;
  content?: unknown;
  toolCalls?: {
    id: string;
    type: "function";
    function: { name: string; arguments: string };
  }[];
  toolCallId?: string;
  [key: string]: unknown;
};

type EventLike = {
  type: string;
  input?: { messages?: FoldMessage[] };
  messages?: FoldMessage[];
  messageId?: string;
  delta?: string;
  role?: string;
  toolCallId?: string;
  toolCallName?: string;
  parentMessageId?: string;
  content?: string;
};

export function foldEventsToMessages(
  existing: readonly FoldMessage[],
  events: readonly EventLike[],
): FoldMessage[] {
  let messages: FoldMessage[] = [...existing];
  let indexById = new Map<string, number>();
  const reindex = () => {
    indexById = new Map(messages.map((m, i) => [m.id, i]));
  };
  reindex();
  const replace = (message: FoldMessage) => {
    const at = indexById.get(message.id);
    if (at === undefined) {
      messages = [...messages, message];
      indexById.set(message.id, messages.length - 1);
    } else {
      messages = [...messages];
      messages[at] = message;
    }
  };
  const assistant = (id: string): FoldMessage => {
    const at = indexById.get(id);
    const current = at === undefined ? undefined : messages[at];
    if (current && current.role === "assistant") return current;
    const created: FoldMessage = { id, role: "assistant", content: "", toolCalls: [] };
    replace(created);
    return created;
  };

  for (const event of events) {
    switch (event.type) {
      case EventType.RUN_STARTED: {
        // A run's input carries the conversation it started from. Merge it in
        // rather than replacing the transcript: the hosted platform appends the
        // input, and a run that starts from a partial (or empty) transcript —
        // a routine, a retry, a client whose restored history dropped a turn —
        // must not erase what the thread already holds. An id seen before is
        // replaced in place, so an input that repeats history keeps its order.
        const input = event.input?.messages;
        if (Array.isArray(input)) {
          for (const message of input) {
            if (!message || typeof message.id !== "string") {
              messages = [...messages, message as FoldMessage];
              continue;
            }
            const at = indexById.get(message.id);
            if (at === undefined) {
              messages = [...messages, message];
              indexById.set(message.id, messages.length - 1);
            } else {
              messages = [...messages];
              messages[at] = message;
            }
          }
        }
        break;
      }
      case EventType.MESSAGES_SNAPSHOT: {
        if (Array.isArray(event.messages)) {
          messages = [...event.messages];
          reindex();
        }
        break;
      }
      case EventType.TEXT_MESSAGE_START: {
        if (!event.messageId) break;
        const role =
          event.role && event.role !== "assistant" ? event.role : "assistant";
        replace({ id: event.messageId, role, content: "" });
        break;
      }
      case EventType.TEXT_MESSAGE_CONTENT: {
        if (!event.messageId) break;
        const at = indexById.get(event.messageId);
        const current = at === undefined ? undefined : messages[at];
        if (!current) {
          replace({ id: event.messageId, role: "assistant", content: event.delta ?? "" });
          break;
        }
        replace({
          ...current,
          content: `${(current.content as string) ?? ""}${event.delta ?? ""}`,
        });
        break;
      }
      case EventType.TOOL_CALL_START: {
        if (!event.toolCallId) break;
        const parent = assistant(event.parentMessageId ?? event.toolCallId);
        replace({
          ...parent,
          toolCalls: [
            ...(parent.toolCalls ?? []),
            {
              id: event.toolCallId,
              type: "function",
              function: { name: event.toolCallName ?? "", arguments: "" },
            },
          ],
        });
        break;
      }
      case EventType.TOOL_CALL_ARGS: {
        if (!event.toolCallId) break;
        for (const message of messages) {
          const call = message.toolCalls?.find((c) => c.id === event.toolCallId);
          if (call) {
            call.function.arguments += event.delta ?? "";
            break;
          }
        }
        break;
      }
      case EventType.TOOL_CALL_RESULT: {
        replace({
          id: `${event.toolCallId}-result`,
          role: "tool",
          content: event.content ?? "",
          toolCallId: event.toolCallId,
        });
        break;
      }
      default:
        break;
    }
  }
  return messages;
}
