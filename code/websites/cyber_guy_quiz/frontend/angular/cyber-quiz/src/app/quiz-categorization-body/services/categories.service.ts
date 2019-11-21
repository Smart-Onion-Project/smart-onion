import { EventEmitter, Output, Injectable } from "@angular/core";
import { CategoryInfo } from "../../models/category-info/category-info.model";
import { BackendService } from "../../services/backend.service";

@Injectable()
export class CategoriesService {
    @Output() categoryAdded : EventEmitter<CategoryInfo> = new EventEmitter<CategoryInfo>();

    private categories : CategoryInfo[] = [];

    constructor(private backendService : BackendService) {}

    addCategory(categoryInfo : CategoryInfo) {
        this.categories.push(categoryInfo);
        this.categoryAdded.emit(categoryInfo);
    }

    getCategories() {
        return this.categories.concat();
    }

    refreshCategories() {
        var raw_categories = this.backendService.getAllCategories();
        raw_categories.map((category_info : CategoryInfo) => {
            this.addCategory(category_info);
        });
    }
}