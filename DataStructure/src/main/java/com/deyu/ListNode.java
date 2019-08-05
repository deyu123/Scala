package com.deyu;

public class ListNode {
    private int element;
    private ListNode next;

    public ListNode(int element) {
        this.element = element;
    }

    public int getElement() {
        return element;
    }

    public void setElement(int element) {
        this.element = element;
    }

    public ListNode getNext() {
        return next;
    }

    public void setNext(ListNode next) {
        this.next = next;
    }

    /**
     * 遍历法：将当前节点cur的下一个节点 cur.getNext()缓存到temp后，
     * 然后更改当前节点指针指向上一结点pre。也就是说在反转当前结点指针指向前，先把当前结点的指针域用tmp临时保存，
     * 以便下一次使用，其过程可表示如下：
     * pre：上一结点
     * cur: 当前结点
     * tmp: 临时结点，用于保存当前结点的指针域（即下一结点）
     *
     * @param head
     * @return
     */
    public static ListNode reverse(ListNode head) {
        if (null == head || null == head.getNext()) {
            return head;
        }
        ListNode pre = head;
        ListNode cur = head.getNext();
        ListNode tmp;
        //当cur = null时候 cur为head节点
        while (null != cur) {
            tmp = cur.getNext();
            cur.setNext(pre);
            //指针向下移动
            pre = cur;
            cur = tmp;
        }
        //将原链表的head置空,还回新链表的头结点，即原链表的尾结点
        head.setNext(null);
        head = pre;
        return head;
    }

    public static ListNode reverse2(ListNode head) {
        //若节点为空直接返回，或者是尾节点也返回
        if (head == null || head.getNext() == null) {
            return head;
        }
        //反转后继节点
        ListNode reverseHead = reverse2(head.getNext());
        //当前节点指向前一节点
        head.getNext().setNext(head);
        //零前一节点的指针域为空
        head.setNext(null);
        return reverseHead;
    }
}
