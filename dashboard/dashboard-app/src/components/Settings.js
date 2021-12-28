import React from "react";

import OrderGroup from './QueryBuilder/OrderGroup';
import { Button, Popover } from 'antd';

export default function Settings({
  orderMembers,
  disabled,
  onReorder,
  onOrderChange,
  isQueryPresent,
}) {

  return (
    <>
      <Popover
        content={
          <div
            style={{
              padding: '8px',
              paddingBottom: 1,
            }}
          >
            <OrderGroup
              orderMembers={orderMembers}
              onReorder={onReorder}
              onOrderChange={onOrderChange}
            />
          </div>
        }
        placement="bottomLeft"
        trigger="click"
      >
        <Button
          data-testid="order-btn"
          disabled={!isQueryPresent || disabled}
          style={{ border: 0 }}
        >
          Order
        </Button>
      </Popover>
    </>
  );
}
