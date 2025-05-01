
import React, { useState } from 'react';
import { Button } from "@/components/ui/button";
import { Menu, X } from "lucide-react";
import { cn } from "@/lib/utils";

const Navbar = () => {
  const [isMenuOpen, setIsMenuOpen] = useState(false);

  const toggleMenu = () => {
    setIsMenuOpen(!isMenuOpen);
  };

  return (
    <nav className="bg-background py-4 px-6 border-b border-border">
      <div className="container flex justify-between items-center">
        <a href="/" className="text-xl font-semibold">
          My Project
        </a>
        
        {/* Desktop Nav */}
        <div className="hidden md:flex items-center space-x-6">
          <a href="/" className="text-foreground hover:text-primary transition-colors">
            Home
          </a>
          <a href="#about" className="text-foreground hover:text-primary transition-colors">
            About
          </a>
          <a href="#services" className="text-foreground hover:text-primary transition-colors">
            Services
          </a>
          <a href="#contact" className="text-foreground hover:text-primary transition-colors">
            Contact
          </a>
          <Button variant="outline" size="sm">
            Get Started
          </Button>
        </div>
        
        {/* Mobile Menu Button */}
        <Button 
          variant="ghost" 
          size="icon" 
          className="md:hidden"
          onClick={toggleMenu}
          aria-label="Toggle menu"
        >
          {isMenuOpen ? <X size={20} /> : <Menu size={20} />}
        </Button>
      </div>

      {/* Mobile Menu */}
      <div className={cn(
        "fixed inset-0 bg-background z-50 flex flex-col pt-16 px-6 md:hidden transition-transform duration-300",
        isMenuOpen ? "translate-x-0" : "translate-x-full"
      )}>
        <div className="flex flex-col space-y-6 items-center animate-fade-in">
          <a 
            href="/" 
            className="text-lg font-medium w-full text-center py-2 hover:bg-muted rounded-md"
            onClick={toggleMenu}
          >
            Home
          </a>
          <a 
            href="#about" 
            className="text-lg font-medium w-full text-center py-2 hover:bg-muted rounded-md"
            onClick={toggleMenu}
          >
            About
          </a>
          <a 
            href="#services" 
            className="text-lg font-medium w-full text-center py-2 hover:bg-muted rounded-md"
            onClick={toggleMenu}
          >
            Services
          </a>
          <a 
            href="#contact" 
            className="text-lg font-medium w-full text-center py-2 hover:bg-muted rounded-md"
            onClick={toggleMenu}
          >
            Contact
          </a>
          <Button className="w-full mt-4">
            Get Started
          </Button>
        </div>
      </div>
    </nav>
  );
};

export default Navbar;
