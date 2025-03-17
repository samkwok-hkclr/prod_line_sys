from .dispenser_station import DispenserStation
from .conveyor_segment import ConveyorSegment
from typing import Optional

class Conveyor:
    def __init__(self):
        self.head = None
        self.tail = None

    def append(self, id):
        new_node = ConveyorSegment(id)
        if self.head == None:
            self.head = new_node
            self.tail = new_node
        else:
            self.tail.next = new_node
            self.tail = self.tail.next

    def delete(self):
        if self.head == None:
            return

        if len(self) == 1:
            self.head = None
            self.tail = None
        else:
            curr_node = self.head
            while curr_node.next != self.tail:
                curr_node = curr_node.next
            curr_node.next = None
            self.tail = curr_node

    def insert(self, index, id):
        if not self.head or not (1 <= index <= len(self)):
            return
            
        new_node = ConveyorSegment(id)
        if index == 1:
            new_node.next = self.head
            self.head = new_node
        else:
            curr_idx = 1
            curr_node = self.head
            while curr_idx + 1 != index:
                curr_node = curr_node.next
                curr_idx += 1
            new_node.next = curr_node.next
            curr_node = new_node
            if not new_node.next:
                self.tail = new_node

    def get_conveyor(self, id):
        """Return the ConveyorSegment with the specified id, or None if not found"""
        if not self.head:
            return None
        
        curr_node = self.head
        while curr_node:
            if curr_node.id == id:
                return curr_node
            curr_node = curr_node.next

        return None
    
    def get_last_conveyor(self, id: int) -> Optional[ConveyorSegment]:
        """Return the ConveyorSegment that comes before the segment with the specified id,
        or None if not found or if it's the first segment"""
        if not self.head or not self.head.next:
            return None
        
        curr_node = self.head
        while curr_node.next:
            if curr_node.next.id == id:
                return curr_node
            curr_node = curr_node.next
    
        return None
    
    def get_next_conveyor(self, id: int) -> Optional[ConveyorSegment]:
        """Return the ConveyorSegment that comes after the segment with the specified id,
        or None if not found or if it's the last segment"""
        if not self.head:
            return None
        
        curr_node = self.head
        while curr_node:
            if curr_node.id == id:
                return curr_node.next
            curr_node = curr_node.next
            
        return None

    def attach_station(self, index, side, station):
        """Attach a dispenser station to a specific segment"""
        if not (1 <= index <= len(self)):
            return
        curr_node = self.head
        curr_idx = 1
        while curr_idx < index:
            curr_node = curr_node.next
            curr_idx += 1
        if side.lower() == 'left':
            curr_node.l_station = station
        elif side.lower() == 'right':
            curr_node.r_station = station

    def detach_station(self, index, side):
        """Remove a dispenser station from a specific segment"""
        if not (1 <= index <= len(self)):
            return
        curr_node = self.head
        curr_idx = 1
        while curr_idx < index:
            curr_node = curr_node.next
            curr_idx += 1
        if side.lower() == 'left':
            curr_node.l_station = None
        elif side.lower() == 'right':
            curr_node.r_station = None

    def get_station(self, id: int) -> Optional[DispenserStation]:
        """Return the DispenserStation with id"""
        if not self.head:
            return None      
        curr_node = self.head
        while curr_node:
            if curr_node.l_station and curr_node.l_station.id == id:
                return curr_node.l_station
            if curr_node.r_station and curr_node.r_station.id == id:
                return curr_node.r_station
            curr_node = curr_node.next
        return None 
    
    def __len__(self):
        length = 0
        curr_node = self.head
        while curr_node:
            length += 1
            curr_node = curr_node.next
        return length

    def __str__(self):
        if not self.head:
            return "Empty Conveyor"
        curr_node = self.head
        chain = []
        index = 1
        while curr_node:
            segment_info = f"[{index}, id:{curr_node.id}"
            if curr_node.l_station:
                segment_info += f", L:{curr_node.l_station}"
            if curr_node.r_station:
                segment_info += f", R:{curr_node.r_station}"
            segment_info += "]"
            chain.append(segment_info)
            index += 1
            curr_node = curr_node.next
        return " --> \n".join(chain)
    