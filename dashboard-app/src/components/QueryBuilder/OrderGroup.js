import React from 'react';
import * as PropTypes from 'prop-types';
import { DragDropContext, Droppable } from 'react-beautiful-dnd';
import DraggableItem from './DraggableItem';

const OrderGroup = ({
  orderMembers, onOrderChange, onReorder
}) => (
    <DragDropContext
      onDragEnd={({ source, destination }) => {
        onReorder(source && source.index, destination && destination.index);
      }}
    >
      <Droppable droppableId="droppable">
        {(provided) => (
          <div
            data-testid="order-popover"
            ref={provided.innerRef}
            {...provided.droppableProps}
          >
            {orderMembers.map(({ id, title, order }, index) => (
              <DraggableItem
                key={id}
                id={id}
                index={index}
                order={order}
                onOrderChange={onOrderChange}
              >
                {title}
              </DraggableItem>
            ))}

            {provided.placeholder}
          </div>
        )}
      </Droppable>
    </DragDropContext>
);

OrderGroup.propTypes = {
  orderMembers: PropTypes.array.isRequired,
  onOrderChange: PropTypes.array.isRequired,
  onReorder: PropTypes.string.isRequired,
};

export default OrderGroup;
