import { Component, OnInit, Input, OnDestroy } from '@angular/core';
import { SlideBarData } from '../../../quiz-categorization-body/models/slide-bar-data/slide-bar-data.model';
import { SlideBarDataUpdate } from '../../../quiz-categorization-body/models/slide-bar-data-update/slide-bar-data-update.model';
import { v4 as uuid } from 'uuid';
import { CategoriesService } from '../../../quiz-categorization-body/services/categories.service';
import { CategoryInfo } from '../../../models/category-info/category-info.model';
import { isUndefined } from 'util';

// TODO: 
// 0. Create the service and create event that will be emitted when the list of the categories is being retrieved from the backend service 
//    the slidebar-group will subscribe to this event to populate its list of slidebars
// 1. Configure this component to get the list of slidebars data from the parent component one by one.
// 2. Build a service for connecting to backend and get the data from it.
// 3. Build the code in the parent component that will pull the next quiz query and the list of categories and populate the relevant controls
// 4. Allow the user to add a category
// 5. Test
// 6. Fill in the database to create more quiz questions
// 7. Create the next quiz' structure (3D "board" for placing anomalies indications)
@Component({
  selector: 'app-slide-bar-group',
  templateUrl: './slide-bar-group.component.html',
  styleUrls: ['./slide-bar-group.component.scss']
})
export class SlideBarGroupComponent implements OnInit, OnDestroy {
  @Input() slidebarsMinValue : number = 0;
  @Input() slidebarsMaxValue : number = 20;
  private items : SlideBarData[] = [];

  constructor(private categoriesService : CategoriesService) {
    this.categoriesService.categoryAdded.subscribe((categoryInfo : CategoryInfo) => {
      this.add_slidebar(categoryInfo);
    });
  }

  ngOnInit() {
  }

  ngOnDestroy() {
    this.categoriesService.categoryAdded.unsubscribe();
  }

  get_total_score() {
    var sum = 0;

    this.items.forEach(element => {
      sum += element.value;
    });

    return sum;
  }

  update_list(id : string, value : number) {
    this.items.forEach(element => {
      if (element.id == id) {
        element.value = value;
        element.touched = true;
      }
    });
  }

  slidebarValueModified(eventDetails : SlideBarDataUpdate) {
    this.update_list(eventDetails.id, eventDetails.value);
  }

  autoSetSlidebars(userModifiedSlideBarId : string, modificationFactor : number) {
    var elementToUpdate : SlideBarData = undefined;

    //Find a slidebar that hasn't been touched by the user and hasn't been modified by the group and update it
    elementToUpdate = this.items.find(sliderData => ! sliderData.touched && sliderData.timesModifiedByGroup == 0);
    if (elementToUpdate != undefined) {
      elementToUpdate.value += modificationFactor;
      elementToUpdate.timesModifiedByGroup += 1;
      return;
    }

    //If no such slider could be found, find a slider that hasn't been touched by the user and has been changed by the group the lowest amount of times so far
    var elementsMatching : SlideBarData[] = this.items.filter(sliderData => ! sliderData.touched).concat().sort((a : SlideBarData, b : SlideBarData) : number => {
      if (a.timesModifiedByGroup > b.timesModifiedByGroup) {
        return 1;
      }
      else if (a.timesModifiedByGroup < b.timesModifiedByGroup) {
        return -1;
      }
      else {
        return 0;
      }
    });
    if (elementsMatching.length > 0) {
      elementToUpdate = elementsMatching[0];
      elementToUpdate.value += modificationFactor;
      elementToUpdate.timesModifiedByGroup += 1;
      return;
    }

    //If no such slider could be found, find a slider that has been changed by the group the lowest amount of times so far
    var elementsMatching : SlideBarData[] = this.items.concat().sort((a : SlideBarData, b : SlideBarData) : number => {
      if (a.timesModifiedByGroup > b.timesModifiedByGroup) {
        return 1;
      }
      else if (a.timesModifiedByGroup < b.timesModifiedByGroup) {
        return -1;
      }
      else {
        return 0;
      }
    });
    if (elementsMatching.length > 0) {
      if (elementsMatching[0].id != userModifiedSlideBarId) {
        elementToUpdate = elementsMatching[0];
      }
      else {
        elementToUpdate = elementsMatching[1];
      }

      if (elementToUpdate != null && ! isUndefined(elementToUpdate)) {
        elementToUpdate.value += modificationFactor;
        elementToUpdate.timesModifiedByGroup += 1;
      }
      return;
    }
  }

  slidebarValueIncreased(eventDetails : SlideBarDataUpdate) {
    this.autoSetSlidebars(eventDetails.id, -1)
  }

  slidebarValueDecreased(eventDetails : SlideBarDataUpdate) {
    this.autoSetSlidebars(eventDetails.id, +1);
   }

  add_slidebar(categoryInfo : CategoryInfo) : boolean {
    if (! this.items.find(slidebar => slidebar.id == categoryInfo.categoryId || slidebar.label == categoryInfo.categoryName)) {
      var new_slider = new SlideBarData(categoryInfo.categoryId, categoryInfo.categoryName, Math.floor(this.slidebarsMaxValue / 2) ,this.slidebarsMinValue, this.slidebarsMaxValue, false, 0);
      this.items.push(new_slider);
      return true;
    }
    else {
      return false;
    }
  }
}
